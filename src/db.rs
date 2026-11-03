//! SQLite storage: accounts, the mailbox, open tells and the team memory.
//!
//! Everything here is synchronous and takes a plain `Connection`, so the
//! server and the admin subcommands share the same code.

use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Map, Value};

pub const DB_FILE: &str = "crew-router.db";

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS accounts (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    name_key    TEXT NOT NULL UNIQUE,
    token_hash  TEXT NOT NULL UNIQUE,
    profile     TEXT NOT NULL DEFAULT '{}',
    created_at  INTEGER NOT NULL,
    last_seen   INTEGER
);

-- Frames waiting for an ack. `item` never repeats (AUTOINCREMENT), so a
-- client can remember which items it already handled.
CREATE TABLE IF NOT EXISTS mailbox (
    item        INTEGER PRIMARY KEY AUTOINCREMENT,
    recipient   TEXT NOT NULL,
    kind        TEXT NOT NULL,
    task_id     TEXT NOT NULL,
    sender      TEXT NOT NULL,
    payload     TEXT NOT NULL,
    delivered   INTEGER NOT NULL DEFAULT 0,
    created_at  INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS mailbox_recipient ON mailbox (recipient, item);
CREATE INDEX IF NOT EXISTS mailbox_expires ON mailbox (expires_at);

-- Which account sent each open tell, so progress/reply/done find their way back.
CREATE TABLE IF NOT EXISTS tasks (
    recipient   TEXT NOT NULL,
    id          TEXT NOT NULL,
    sender      TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL,
    PRIMARY KEY (recipient, id)
);
CREATE UNIQUE INDEX IF NOT EXISTS tasks_sender ON tasks (sender, id);
CREATE INDEX IF NOT EXISTS tasks_expires ON tasks (expires_at);

CREATE TABLE IF NOT EXISTS team_memory (
    one         INTEGER PRIMARY KEY CHECK (one = 1),
    text        TEXT NOT NULL,
    version     INTEGER NOT NULL,
    updated_by  TEXT,
    updated_at  INTEGER
);
INSERT OR IGNORE INTO team_memory (one, text, version) VALUES (1, '', 0);
"#;

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Opens (and creates, if needed) the database in `dir`.
pub fn open(dir: &Path) -> Result<Connection> {
    std::fs::create_dir_all(dir)
        .with_context(|| format!("cannot create data dir {}", dir.display()))?;
    let path = dir.join(DB_FILE);
    let conn =
        Connection::open(&path).with_context(|| format!("cannot open {}", path.display()))?;
    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.execute_batch(SCHEMA)?;
    Ok(conn)
}

#[derive(Debug, Clone)]
pub struct Account {
    pub id: String,
    pub name: String,
    pub profile: Map<String, Value>,
    pub created_at: i64,
    pub last_seen: Option<i64>,
}

fn account_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Account> {
    let profile: String = row.get(2)?;
    Ok(Account {
        id: row.get(0)?,
        name: row.get(1)?,
        profile: serde_json::from_str(&profile).unwrap_or_default(),
        created_at: row.get(3)?,
        last_seen: row.get(4)?,
    })
}

const ACCOUNT_COLS: &str = "id, name, profile, created_at, last_seen";

pub fn create_account(
    conn: &Connection,
    id: &str,
    name: &str,
    name_key: &str,
    token_hash: &str,
    profile: &Map<String, Value>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO accounts (id, name, name_key, token_hash, profile, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            id,
            name,
            name_key,
            token_hash,
            Value::Object(profile.clone()).to_string(),
            now_ms()
        ],
    )?;
    Ok(())
}

pub fn account(conn: &Connection, id: &str) -> Result<Option<Account>> {
    Ok(conn
        .query_row(
            &format!("SELECT {ACCOUNT_COLS} FROM accounts WHERE id = ?1"),
            [id],
            account_from_row,
        )
        .optional()?)
}

pub fn account_by_token_hash(conn: &Connection, hash: &str) -> Result<Option<Account>> {
    Ok(conn
        .query_row(
            &format!("SELECT {ACCOUNT_COLS} FROM accounts WHERE token_hash = ?1"),
            [hash],
            account_from_row,
        )
        .optional()?)
}

pub fn account_by_name_key(conn: &Connection, key: &str) -> Result<Option<Account>> {
    Ok(conn
        .query_row(
            &format!("SELECT {ACCOUNT_COLS} FROM accounts WHERE name_key = ?1"),
            [key],
            account_from_row,
        )
        .optional()?)
}

pub fn accounts(conn: &Connection) -> Result<Vec<Account>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {ACCOUNT_COLS} FROM accounts ORDER BY created_at, id"
    ))?;
    let rows = stmt.query_map([], account_from_row)?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

pub fn set_profile(conn: &Connection, id: &str, profile: &Map<String, Value>) -> Result<()> {
    conn.execute(
        "UPDATE accounts SET profile = ?2 WHERE id = ?1",
        params![id, Value::Object(profile.clone()).to_string()],
    )?;
    Ok(())
}

pub fn touch(conn: &Connection, id: &str) -> Result<()> {
    conn.execute(
        "UPDATE accounts SET last_seen = ?2 WHERE id = ?1",
        params![id, now_ms()],
    )?;
    Ok(())
}

pub fn rename(conn: &Connection, id: &str, name: &str, name_key: &str) -> Result<()> {
    conn.execute(
        "UPDATE accounts SET name = ?2, name_key = ?3 WHERE id = ?1",
        params![id, name, name_key],
    )?;
    Ok(())
}

/// Deletes an account and settles everything that pointed at it. Every open
/// tell addressed to the account becomes `done{ok:false,error:"recipient_gone"}`
/// in its sender's mailbox. Returns the senders that got one.
pub fn delete_account(conn: &mut Connection, id: &str, ttl_ms: i64) -> Result<Vec<String>> {
    let tx = conn.transaction()?;
    let open: Vec<(String, String)> = {
        let mut stmt = tx.prepare("SELECT id, sender FROM tasks WHERE recipient = ?1")?;
        let rows = stmt.query_map([id], |r| Ok((r.get(0)?, r.get(1)?)))?;
        rows.collect::<rusqlite::Result<_>>()?
    };
    let mut senders = Vec::new();
    for (task, sender) in open {
        if sender == id {
            continue;
        }
        let frame = json!({
            "type": "done", "id": task, "from": id, "ok": false, "error": "recipient_gone",
        });
        insert_item(&tx, &sender, "done", &task, id, &frame, ttl_ms)?;
        if !senders.contains(&sender) {
            senders.push(sender);
        }
    }
    tx.execute(
        "DELETE FROM tasks WHERE recipient = ?1 OR sender = ?1",
        [id],
    )?;
    tx.execute("DELETE FROM mailbox WHERE recipient = ?1", [id])?;
    // Tells it sent that nobody has seen yet would only make work for no one.
    tx.execute(
        "DELETE FROM mailbox WHERE sender = ?1 AND kind = 'tell' AND delivered = 0",
        [id],
    )?;
    tx.execute("DELETE FROM accounts WHERE id = ?1", [id])?;
    tx.commit()?;
    Ok(senders)
}

// ---------------------------------------------------------------- mailbox

/// Stores a frame for `recipient`. `frame` is what will be sent, minus `item`.
pub fn insert_item(
    conn: &Connection,
    recipient: &str,
    kind: &str,
    task_id: &str,
    sender: &str,
    frame: &Value,
    ttl_ms: i64,
) -> Result<i64> {
    let now = now_ms();
    conn.execute(
        "INSERT INTO mailbox (recipient, kind, task_id, sender, payload, created_at, expires_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            recipient,
            kind,
            task_id,
            sender,
            frame.to_string(),
            now,
            now + ttl_ms
        ],
    )?;
    Ok(conn.last_insert_rowid())
}

/// Items for `recipient` newer than `after`, oldest first, with `item` filled in.
pub fn items_after(conn: &Connection, recipient: &str, after: i64) -> Result<Vec<(i64, String)>> {
    let mut stmt = conn.prepare(
        "SELECT item, payload FROM mailbox WHERE recipient = ?1 AND item > ?2 ORDER BY item",
    )?;
    let rows = stmt.query_map(params![recipient, after], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (item, payload) = row?;
        let mut frame: Value = serde_json::from_str(&payload)?;
        if let Some(obj) = frame.as_object_mut() {
            obj.insert("item".into(), json!(item));
        }
        out.push((item, frame.to_string()));
    }
    Ok(out)
}

pub fn mark_delivered(conn: &Connection, recipient: &str, after: i64, upto: i64) -> Result<()> {
    conn.execute(
        "UPDATE mailbox SET delivered = 1 WHERE recipient = ?1 AND item > ?2 AND item <= ?3",
        params![recipient, after, upto],
    )?;
    Ok(())
}

pub fn ack(conn: &Connection, recipient: &str, item: i64) -> Result<bool> {
    Ok(conn.execute(
        "DELETE FROM mailbox WHERE recipient = ?1 AND item = ?2",
        params![recipient, item],
    )? > 0)
}

pub fn mailbox_count(conn: &Connection, recipient: &str) -> Result<i64> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM mailbox WHERE recipient = ?1",
        [recipient],
        |r| r.get(0),
    )?)
}

/// Deletes a tell the recipient has never been sent. True if there was one.
pub fn take_undelivered_tell(
    conn: &Connection,
    recipient: &str,
    task_id: &str,
    sender: &str,
) -> Result<bool> {
    Ok(conn.execute(
        "DELETE FROM mailbox WHERE recipient = ?1 AND kind = 'tell' AND task_id = ?2
         AND sender = ?3 AND delivered = 0",
        params![recipient, task_id, sender],
    )? > 0)
}

// ------------------------------------------------------------------ tasks

#[derive(Debug, Clone)]
pub struct Task {
    pub recipient: String,
    pub id: String,
    pub sender: String,
}

fn task_from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Task> {
    Ok(Task {
        recipient: r.get(0)?,
        id: r.get(1)?,
        sender: r.get(2)?,
    })
}

pub fn insert_task(
    conn: &Connection,
    recipient: &str,
    id: &str,
    sender: &str,
    ttl_ms: i64,
) -> Result<()> {
    let now = now_ms();
    conn.execute(
        "INSERT INTO tasks (recipient, id, sender, created_at, expires_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![recipient, id, sender, now, now + ttl_ms],
    )?;
    Ok(())
}

pub fn task_for_recipient(conn: &Connection, recipient: &str, id: &str) -> Result<Option<Task>> {
    Ok(conn
        .query_row(
            "SELECT recipient, id, sender FROM tasks WHERE recipient = ?1 AND id = ?2",
            [recipient, id],
            task_from_row,
        )
        .optional()?)
}

pub fn task_for_sender(conn: &Connection, sender: &str, id: &str) -> Result<Option<Task>> {
    Ok(conn
        .query_row(
            "SELECT recipient, id, sender FROM tasks WHERE sender = ?1 AND id = ?2",
            [sender, id],
            task_from_row,
        )
        .optional()?)
}

pub fn delete_task(conn: &Connection, recipient: &str, id: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM tasks WHERE recipient = ?1 AND id = ?2",
        [recipient, id],
    )?;
    Ok(())
}

// ------------------------------------------------------------ team memory

#[derive(Debug, Clone)]
pub struct TeamMemory {
    pub text: String,
    pub version: i64,
    pub updated_by: Option<String>,
}

pub fn team_memory(conn: &Connection) -> Result<TeamMemory> {
    Ok(conn.query_row(
        "SELECT text, version, updated_by FROM team_memory WHERE one = 1",
        [],
        |r| {
            Ok(TeamMemory {
                text: r.get(0)?,
                version: r.get(1)?,
                updated_by: r.get(2)?,
            })
        },
    )?)
}

pub fn set_team_memory(conn: &Connection, text: &str, by: &str) -> Result<TeamMemory> {
    conn.execute(
        "UPDATE team_memory SET text = ?1, version = version + 1, updated_by = ?2, updated_at = ?3
         WHERE one = 1",
        params![text, by, now_ms()],
    )?;
    team_memory(conn)
}

// ------------------------------------------------------------ housekeeping

/// What one purge removed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Purged {
    pub items: usize,
    pub tasks: usize,
    /// Expired tells whose sender got `done{ok:false,error:"expired"}`.
    pub expired: usize,
}

/// Drops expired mailbox items and tell routes.
///
/// A tell that runs out of time still sitting in its recipient's mailbox goes
/// back to its sender as `done{ok:false,error:"expired"}` (stored for
/// `ttl_ms`, like any other item) and its route closes. If the sender's
/// account is gone, the notice is dropped. Other expired items just go.
pub fn purge(conn: &mut Connection, now: i64, ttl_ms: i64) -> Result<Purged> {
    let tx = conn.transaction()?;
    let stale: Vec<(i64, String, String, String)> = {
        let mut stmt = tx.prepare(
            "SELECT m.item, t.recipient, t.id, t.sender FROM tasks t
             JOIN mailbox m ON m.recipient = t.recipient AND m.task_id = t.id
                 AND m.sender = t.sender AND m.kind = 'tell'
             WHERE m.expires_at < ?1 OR t.expires_at < ?1
             ORDER BY m.item",
        )?;
        let rows = stmt.query_map([now], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))?;
        rows.collect::<rusqlite::Result<_>>()?
    };
    let mut out = Purged::default();
    for (item, recipient, task, sender) in stale {
        out.items += tx.execute("DELETE FROM mailbox WHERE item = ?1", [item])?;
        delete_task(&tx, &recipient, &task)?;
        out.tasks += 1;
        if account(&tx, &sender)?.is_none() {
            continue;
        }
        let frame = json!({
            "type": "done", "id": task, "from": recipient, "ok": false, "error": "expired",
        });
        insert_item(&tx, &sender, "done", &task, &recipient, &frame, ttl_ms)?;
        out.expired += 1;
    }
    out.items += tx.execute("DELETE FROM mailbox WHERE expires_at < ?1", [now])?;
    out.tasks += tx.execute("DELETE FROM tasks WHERE expires_at < ?1", [now])?;
    tx.commit()?;
    Ok(out)
}

#[derive(Debug, Clone)]
pub struct Stats {
    pub accounts: i64,
    pub mailbox_items: i64,
    pub mailbox_bytes: i64,
    pub open_tells: i64,
    pub team_memory_version: i64,
    pub team_memory_bytes: i64,
}

pub fn stats(conn: &Connection) -> Result<Stats> {
    let accounts = conn.query_row("SELECT COUNT(*) FROM accounts", [], |r| r.get(0))?;
    let (mailbox_items, mailbox_bytes) = conn.query_row(
        "SELECT COUNT(*), COALESCE(SUM(LENGTH(CAST(payload AS BLOB))), 0) FROM mailbox",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let open_tells = conn.query_row("SELECT COUNT(*) FROM tasks", [], |r| r.get(0))?;
    let (team_memory_version, team_memory_bytes) = conn.query_row(
        "SELECT version, LENGTH(CAST(text AS BLOB)) FROM team_memory WHERE one = 1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    Ok(Stats {
        accounts,
        mailbox_items,
        mailbox_bytes,
        open_tells,
        team_memory_version,
        team_memory_bytes,
    })
}

pub fn mailbox_counts(conn: &Connection) -> Result<std::collections::HashMap<String, i64>> {
    let mut stmt = conn.prepare("SELECT recipient, COUNT(*) FROM mailbox GROUP BY recipient")?;
    let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}
