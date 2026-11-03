//! The wire protocol: who is online, and what happens to each frame.
//!
//! All state sits behind one mutex. A frame is handled start to finish while
//! holding it, so frames to one connection go out in the order they were
//! produced, and mailbox items reach a client in `item` order.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use base64::Engine;
use rusqlite::Connection;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::db;
use crate::ids;

pub const PROTOCOL: u64 = 1;
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Largest WebSocket message the router reads.
pub const MAX_FRAME_BYTES: usize = 32 * 1024 * 1024;
/// Largest attachment, decoded.
pub const MAX_FILE_BYTES: u64 = 25 * 1024 * 1024;
pub const TELLS_PER_MINUTE: usize = 60;
pub const MAILBOX_LIMIT: i64 = 1000;
/// Cap on a profile, all fields together.
const MAX_PROFILE_BYTES: usize = 256 * 1024;
const MAX_ID_CHARS: usize = 256;
const PROFILE_KEYS: [&str; 4] = ["role", "avatar", "avatar_shape", "avatar_color"];
const STATUSES: [&str; 3] = ["idle", "working", "asking"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignupMode {
    Open,
    Invite,
    Closed,
}

impl SignupMode {
    pub fn as_str(self) -> &'static str {
        match self {
            SignupMode::Open => "open",
            SignupMode::Invite => "invite",
            SignupMode::Closed => "closed",
        }
    }
}

#[derive(Debug, Clone)]
pub struct HubConfig {
    pub title: String,
    pub signup: SignupMode,
    pub invite_code: Option<String>,
    /// How long mailbox items and tell routes live.
    pub ttl: Duration,
}

/// What a connection's writer task sends.
#[derive(Debug)]
pub enum Out {
    Frame(String),
    /// Send a close frame and stop.
    Close,
}

pub type Tx = mpsc::UnboundedSender<Out>;

/// One WebSocket connection, as the hub sees it.
pub struct Session {
    pub conn: u64,
    pub tx: Tx,
    pub account: Option<String>,
}

struct Live {
    conn: u64,
    tx: Tx,
    /// Highest mailbox item already sent on this connection.
    cursor: i64,
    status: String,
}

struct Inner {
    db: Connection,
    online: HashMap<String, Live>,
    /// account id → name, as last announced. The tick diffs it against the
    /// database to notice admin kicks and renames.
    roster: HashMap<String, String>,
    tells: HashMap<String, VecDeque<Instant>>,
    next_conn: u64,
}

pub struct Hub {
    cfg: HubConfig,
    inner: Mutex<Inner>,
}

fn send(tx: &Tx, frame: Value) {
    let _ = tx.send(Out::Frame(frame.to_string()));
}

fn error_frame(id: Option<&str>, code: &str, message: &str) -> Value {
    let mut v = json!({ "type": "error", "code": code, "message": message });
    if let Some(id) = id {
        v["id"] = json!(id);
    }
    v
}

/// A reason to answer a frame with `error`.
struct Reject {
    code: &'static str,
    message: String,
}

fn reject(code: &'static str, message: impl Into<String>) -> Reject {
    Reject {
        code,
        message: message.into(),
    }
}

impl From<anyhow::Error> for Reject {
    fn from(e: anyhow::Error) -> Self {
        warn!(error = %e, "storage error");
        reject("internal", "the router could not store that")
    }
}

type Handled = Result<(), Reject>;

fn parse<T: for<'de> Deserialize<'de>>(v: Value) -> Result<T, Reject> {
    serde_json::from_value(v).map_err(|e| reject("bad_frame", e.to_string()))
}

#[derive(Deserialize)]
struct SignupIn {
    name: String,
    #[serde(default)]
    invite: Option<String>,
    #[serde(default)]
    profile: Option<Map<String, Value>>,
}

#[derive(Deserialize)]
struct AuthIn {
    token: String,
}

#[derive(Deserialize)]
struct PresenceIn {
    status: String,
}

#[derive(Deserialize)]
struct TellIn {
    id: String,
    to: String,
    text: String,
    #[serde(default)]
    hops: u64,
    #[serde(default)]
    files: Option<Vec<Value>>,
}

#[derive(Deserialize)]
struct ProgressIn {
    id: String,
    state: String,
    #[serde(default)]
    text: Option<String>,
}

#[derive(Deserialize)]
struct ReplyIn {
    id: String,
    text: String,
    #[serde(default)]
    files: Option<Vec<Value>>,
}

#[derive(Deserialize)]
struct DoneIn {
    id: String,
    ok: bool,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Deserialize)]
struct IdIn {
    id: String,
}

#[derive(Deserialize)]
struct AckIn {
    item: Value,
}

#[derive(Deserialize)]
struct TextIn {
    text: String,
}

#[derive(Deserialize)]
struct SetIn {
    text: String,
    version: i64,
}

fn check_id(id: &str) -> Result<(), Reject> {
    if id.is_empty() || id.chars().count() > MAX_ID_CHARS {
        return Err(reject("bad_frame", "id must be 1–256 characters"));
    }
    Ok(())
}

/// Decoded size of a base64 string, without decoding it.
fn base64_len(data: &str) -> u64 {
    let trimmed = data.trim_end_matches('=');
    (trimmed.len() as u64 * 3) / 4
}

/// Checks each file is shaped right and within 25MB.
fn check_files(files: &Option<Vec<Value>>) -> Result<(), Reject> {
    let Some(files) = files else { return Ok(()) };
    for f in files {
        let Some(obj) = f.as_object() else {
            return Err(reject("bad_frame", "each file must be an object"));
        };
        let Some(data) = obj.get("data").and_then(Value::as_str) else {
            return Err(reject("bad_frame", "each file needs base64 `data`"));
        };
        let claimed = obj.get("size").and_then(Value::as_u64).unwrap_or(0);
        if base64_len(data) > MAX_FILE_BYTES || claimed > MAX_FILE_BYTES {
            return Err(reject("too_large", "a file is larger than 25MB"));
        }
        if base64::engine::general_purpose::STANDARD
            .decode(data.as_bytes())
            .is_err()
        {
            return Err(reject("bad_frame", "file data is not base64"));
        }
    }
    Ok(())
}

/// Applies a profile patch: a string sets a field, `null` clears it, a missing
/// key leaves it alone.
fn merge_profile(
    profile: &mut Map<String, Value>,
    patch: &Map<String, Value>,
) -> Result<(), Reject> {
    for key in PROFILE_KEYS {
        match patch.get(key) {
            None => {}
            Some(Value::Null) => {
                profile.remove(key);
            }
            Some(Value::String(s)) => {
                profile.insert(key.into(), Value::String(s.clone()));
            }
            Some(_) => return Err(reject("bad_frame", format!("`{key}` must be a string"))),
        }
    }
    if Value::Object(profile.clone()).to_string().len() > MAX_PROFILE_BYTES {
        return Err(reject("too_large", "the profile is larger than 256KB"));
    }
    Ok(())
}

fn member(account: &db::Account, live: Option<&Live>) -> Value {
    let mut m = Map::new();
    m.insert("account".into(), json!(account.id));
    m.insert("name".into(), json!(account.name));
    for key in PROFILE_KEYS {
        if let Some(v) = account.profile.get(key) {
            m.insert(key.into(), v.clone());
        }
    }
    m.insert("online".into(), json!(live.is_some()));
    m.insert(
        "status".into(),
        json!(live.map(|l| l.status.as_str()).unwrap_or("idle")),
    );
    Value::Object(m)
}

impl Inner {
    fn ttl_ms(cfg: &HubConfig) -> i64 {
        cfg.ttl.as_millis().min(i64::MAX as u128) as i64
    }

    fn member_of(&self, id: &str) -> Option<Value> {
        let account = db::account(&self.db, id).ok().flatten()?;
        Some(member(&account, self.online.get(id)))
    }

    /// Sends a frame to everyone online except `except`.
    fn broadcast(&self, except: Option<&str>, frame: &Value) {
        let text = frame.to_string();
        for (id, live) in &self.online {
            if Some(id.as_str()) != except {
                let _ = live.tx.send(Out::Frame(text.clone()));
            }
        }
    }

    fn announce(&self, id: &str) {
        if let Some(m) = self.member_of(id) {
            self.broadcast(Some(id), &json!({ "type": "member", "member": m }));
        }
    }

    /// Sends every mailbox item the account's connection has not seen yet.
    fn pump(&mut self, id: &str) {
        let Some(live) = self.online.get_mut(id) else {
            return;
        };
        let items = match db::items_after(&self.db, id, live.cursor) {
            Ok(items) => items,
            Err(e) => {
                warn!(account = id, error = %e, "cannot read mailbox");
                return;
            }
        };
        let Some(last) = items.last().map(|(item, _)| *item) else {
            return;
        };
        for (_, frame) in items {
            let _ = live.tx.send(Out::Frame(frame));
        }
        if let Err(e) = db::mark_delivered(&self.db, id, live.cursor, last) {
            warn!(account = id, error = %e, "cannot mark mailbox delivered");
        }
        live.cursor = last;
    }

    fn rate_ok(&mut self, id: &str) -> bool {
        let now = Instant::now();
        let window = self.tells.entry(id.to_string()).or_default();
        while window
            .front()
            .is_some_and(|t| now.duration_since(*t) >= Duration::from_secs(60))
        {
            window.pop_front();
        }
        if window.len() >= TELLS_PER_MINUTE {
            return false;
        }
        window.push_back(now);
        true
    }

    /// Forgets a deleted account everywhere in memory and tells the others.
    fn forget(&mut self, id: &str) {
        self.roster.remove(id);
        self.tells.remove(id);
        self.broadcast(Some(id), &json!({ "type": "member_left", "account": id }));
    }
}

impl Hub {
    pub fn new(cfg: HubConfig, db: Connection) -> anyhow::Result<Self> {
        let roster = db::accounts(&db)?
            .into_iter()
            .map(|a| (a.id, a.name))
            .collect();
        Ok(Hub {
            cfg,
            inner: Mutex::new(Inner {
                db,
                online: HashMap::new(),
                roster,
                tells: HashMap::new(),
                next_conn: 1,
            }),
        })
    }

    pub fn config(&self) -> &HubConfig {
        &self.cfg
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn open_session(&self, tx: Tx) -> Session {
        let mut inner = self.lock();
        let conn = inner.next_conn;
        inner.next_conn += 1;
        Session {
            conn,
            tx,
            account: None,
        }
    }

    fn server_frame(&self) -> Value {
        json!({
            "type": "server",
            "name": "crew-router",
            "version": VERSION,
            "protocol": PROTOCOL,
            "title": self.cfg.title,
            "signup": self.cfg.signup.as_str(),
        })
    }

    /// Handles one text frame. Returns false when the connection should end.
    pub fn handle(&self, s: &mut Session, text: &str) -> bool {
        let frame: Value = match serde_json::from_str(text) {
            Ok(Value::Object(obj)) => Value::Object(obj),
            _ => {
                send(
                    &s.tx,
                    error_frame(None, "bad_frame", "a frame must be a JSON object"),
                );
                return true;
            }
        };
        let kind = frame
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let id = frame.get("id").and_then(Value::as_str).map(str::to_string);
        debug!(conn = s.conn, kind = %kind, "frame");

        let mut keep = true;
        let result = match (kind.as_str(), s.account.clone()) {
            ("ping", _) => {
                send(&s.tx, json!({ "type": "pong" }));
                Ok(())
            }
            ("hello", _) => {
                send(&s.tx, self.server_frame());
                Ok(())
            }
            ("signup", _) => self.signup(s, frame),
            ("auth", None) => self.auth(s, frame),
            ("auth", Some(_)) => Err(reject("bad_frame", "this connection is signed in")),
            ("profile", Some(me)) => self.profile(&me, frame),
            ("presence", Some(me)) => self.presence(&me, frame),
            ("tell", Some(me)) => self.tell(s, &me, frame),
            ("progress", Some(me)) => self.progress(&me, frame),
            ("reply", Some(me)) => self.reply(&me, frame),
            ("done", Some(me)) => self.done(&me, frame),
            ("cancel", Some(me)) => self.cancel(&me, frame),
            ("ack", Some(me)) => self.ack(&me, frame),
            ("team_memory_append", Some(me)) => self.team_memory_append(&me, frame),
            ("team_memory_set", Some(me)) => self.team_memory_set(s, &me, frame),
            ("leave", Some(me)) => {
                keep = false;
                self.leave(s, &me)
            }
            (
                "profile" | "presence" | "tell" | "progress" | "reply" | "done" | "cancel" | "ack"
                | "team_memory_append" | "team_memory_set" | "leave",
                None,
            ) => Err(reject("unauthorized", "send auth first")),
            _ => Err(reject("bad_frame", "unknown frame type")),
        };
        if let Err(r) = result {
            debug!(conn = s.conn, kind = %kind, code = r.code, "rejected");
            send(&s.tx, error_frame(id.as_deref(), r.code, &r.message));
        }
        keep
    }

    /// The connection ended.
    pub fn disconnect(&self, s: &Session) {
        let Some(me) = &s.account else { return };
        let mut inner = self.lock();
        if inner.online.get(me).is_some_and(|l| l.conn == s.conn) {
            inner.online.remove(me);
            let _ = db::touch(&inner.db, me);
            inner.announce(me);
            info!(account = %me, conn = s.conn, "offline");
        }
    }

    fn signup(&self, s: &Session, frame: Value) -> Handled {
        let req: SignupIn = parse(frame)?;
        match self.cfg.signup {
            SignupMode::Closed => {
                return Err(reject("signup_closed", "this router takes no new members"))
            }
            SignupMode::Invite => {
                let want = self.cfg.invite_code.as_deref().unwrap_or_default();
                match req.invite.as_deref() {
                    Some(code) if !want.is_empty() && code == want => {}
                    Some(_) => {
                        return Err(reject("invite_required", "that invite code is not right"))
                    }
                    None => {
                        return Err(reject(
                            "invite_required",
                            "this router needs an invite code",
                        ))
                    }
                }
            }
            SignupMode::Open => {}
        }
        if !ids::valid_name(&req.name) {
            return Err(reject(
                "bad_name",
                "a name is 1–32 letters, digits, `-` or `_`",
            ));
        }
        let mut profile = Map::new();
        if let Some(patch) = &req.profile {
            merge_profile(&mut profile, patch)?;
        }
        let key = ids::name_key(&req.name);
        let mut inner = self.lock();
        if db::account_by_name_key(&inner.db, &key)?.is_some() {
            return Err(reject("name_taken", "that name is taken"));
        }
        let id = ids::new_account_id();
        let token = ids::new_token();
        db::create_account(
            &inner.db,
            &id,
            &req.name,
            &key,
            &ids::token_hash(&token),
            &profile,
        )?;
        inner.roster.insert(id.clone(), req.name.clone());
        inner.announce(&id);
        info!(account = %id, name = %req.name, conn = s.conn, "signed up");
        send(
            &s.tx,
            json!({ "type": "signed_up", "account": id, "name": req.name, "token": token }),
        );
        Ok(())
    }

    fn auth(&self, s: &mut Session, frame: Value) -> Handled {
        let req: AuthIn = parse(frame)?;
        let mut inner = self.lock();
        let Some(account) = db::account_by_token_hash(&inner.db, &ids::token_hash(&req.token))?
        else {
            return Err(reject("bad_token", "no account has that token"));
        };
        let me = account.id.clone();
        if let Some(old) = inner.online.remove(&me) {
            info!(account = %me, old = old.conn, new = s.conn, "replacing older connection");
            send(
                &old.tx,
                error_frame(None, "replaced", "this account signed in somewhere else"),
            );
            let _ = old.tx.send(Out::Close);
        }
        inner.online.insert(
            me.clone(),
            Live {
                conn: s.conn,
                tx: s.tx.clone(),
                cursor: 0,
                status: "idle".into(),
            },
        );
        s.account = Some(me.clone());
        db::touch(&inner.db, &me)?;
        // The roster may predate an admin rename the tick has not seen yet.
        inner.roster.insert(me.clone(), account.name.clone());

        let members: Vec<Value> = db::accounts(&inner.db)?
            .iter()
            .filter(|a| a.id != me)
            .map(|a| member(a, inner.online.get(&a.id)))
            .collect();
        let memory = db::team_memory(&inner.db)?;
        send(
            &s.tx,
            json!({
                "type": "welcome",
                "account": me,
                "name": account.name,
                "members": members,
                "team_memory": { "text": memory.text, "version": memory.version },
            }),
        );
        inner.pump(&me);
        inner.announce(&me);
        info!(account = %me, name = %account.name, conn = s.conn, "online");
        Ok(())
    }

    fn profile(&self, me: &str, frame: Value) -> Handled {
        let Value::Object(patch) = frame else {
            return Err(reject("bad_frame", "profile must be an object"));
        };
        let inner = self.lock();
        let Some(mut account) = db::account(&inner.db, me)? else {
            return Err(reject("account_gone", "this account no longer exists"));
        };
        merge_profile(&mut account.profile, &patch)?;
        db::set_profile(&inner.db, me, &account.profile)?;
        inner.announce(me);
        Ok(())
    }

    fn presence(&self, me: &str, frame: Value) -> Handled {
        let req: PresenceIn = parse(frame)?;
        if !STATUSES.contains(&req.status.as_str()) {
            return Err(reject("bad_frame", "status is idle, working or asking"));
        }
        let mut inner = self.lock();
        if let Some(live) = inner.online.get_mut(me) {
            if live.status == req.status {
                return Ok(());
            }
            live.status = req.status;
        }
        inner.announce(me);
        Ok(())
    }

    fn tell(&self, s: &Session, me: &str, frame: Value) -> Handled {
        let req: TellIn = parse(frame)?;
        check_id(&req.id)?;
        check_files(&req.files)?;
        let ttl = Inner::ttl_ms(&self.cfg);
        let mut inner = self.lock();
        let Some(to) = db::account_by_name_key(&inner.db, &ids::name_key(&req.to))? else {
            return Err(reject("no_such_member", "nobody here has that name"));
        };
        let online = |inner: &Inner| inner.online.contains_key(&to.id);

        // A tell sent again after a reconnect gets the same answer, once.
        if let Some(task) = db::task_for_sender(&inner.db, me, &req.id)? {
            if task.recipient != to.id {
                return Err(reject("duplicate_id", "that id is already in use"));
            }
            send(
                &s.tx,
                json!({ "type": "accepted", "id": req.id, "queued": !online(&inner) }),
            );
            return Ok(());
        }
        if db::task_for_recipient(&inner.db, &to.id, &req.id)?.is_some() {
            return Err(reject("duplicate_id", "that id is already in use"));
        }
        if db::mailbox_count(&inner.db, &to.id)? >= MAILBOX_LIMIT {
            return Err(reject("mailbox_full", "their mailbox is full"));
        }
        if !inner.rate_ok(me) {
            return Err(reject("rate_limited", "at most 60 tells a minute"));
        }
        let from_name = db::account(&inner.db, me)?
            .map(|a| a.name)
            .unwrap_or_default();
        let mut out = json!({
            "type": "tell",
            "id": req.id,
            "from": me,
            "from_name": from_name,
            "text": req.text,
            "hops": req.hops,
        });
        if let Some(files) = req.files {
            out["files"] = Value::Array(files);
        }
        db::insert_task(&inner.db, &to.id, &req.id, me, ttl)?;
        db::insert_item(&inner.db, &to.id, "tell", &req.id, me, &out, ttl)?;
        let queued = !online(&inner);
        inner.pump(&to.id);
        info!(from = %me, to = %to.id, id = %req.id, queued, "tell");
        send(
            &s.tx,
            json!({ "type": "accepted", "id": req.id, "queued": queued }),
        );
        Ok(())
    }

    fn progress(&self, me: &str, frame: Value) -> Handled {
        let req: ProgressIn = parse(frame)?;
        let inner = self.lock();
        // Progress is never stored: no route or an offline sender drops it.
        let Some(task) = db::task_for_recipient(&inner.db, me, &req.id)? else {
            return Ok(());
        };
        if let Some(live) = inner.online.get(&task.sender) {
            let mut out =
                json!({ "type": "progress", "id": req.id, "from": me, "state": req.state });
            if let Some(text) = req.text {
                out["text"] = json!(text);
            }
            send(&live.tx, out);
        }
        Ok(())
    }

    fn reply(&self, me: &str, frame: Value) -> Handled {
        let req: ReplyIn = parse(frame)?;
        check_files(&req.files)?;
        let ttl = Inner::ttl_ms(&self.cfg);
        let mut inner = self.lock();
        let Some(task) = db::task_for_recipient(&inner.db, me, &req.id)? else {
            return Err(reject("unknown_id", "no open tell has that id"));
        };
        let mut out = json!({ "type": "reply", "id": req.id, "from": me, "text": req.text });
        if let Some(files) = req.files {
            out["files"] = Value::Array(files);
        }
        db::insert_item(&inner.db, &task.sender, "reply", &req.id, me, &out, ttl)?;
        inner.pump(&task.sender);
        debug!(from = %me, to = %task.sender, id = %req.id, "reply");
        Ok(())
    }

    fn done(&self, me: &str, frame: Value) -> Handled {
        let req: DoneIn = parse(frame)?;
        let ttl = Inner::ttl_ms(&self.cfg);
        let mut inner = self.lock();
        let Some(task) = db::task_for_recipient(&inner.db, me, &req.id)? else {
            return Err(reject("unknown_id", "no open tell has that id"));
        };
        let mut out = json!({ "type": "done", "id": req.id, "from": me, "ok": req.ok });
        if let Some(error) = &req.error {
            out["error"] = json!(error);
        }
        db::insert_item(&inner.db, &task.sender, "done", &req.id, me, &out, ttl)?;
        db::delete_task(&inner.db, me, &req.id)?;
        inner.pump(&task.sender);
        info!(from = %me, to = %task.sender, id = %req.id, ok = req.ok, "done");
        Ok(())
    }

    fn cancel(&self, me: &str, frame: Value) -> Handled {
        let req: IdIn = parse(frame)?;
        let ttl = Inner::ttl_ms(&self.cfg);
        let mut inner = self.lock();
        let Some(task) = db::task_for_sender(&inner.db, me, &req.id)? else {
            return Err(reject("unknown_id", "no open tell has that id"));
        };
        if db::take_undelivered_tell(&inner.db, &task.recipient, &req.id, me)? {
            // The recipient never saw it: settle it here.
            db::delete_task(&inner.db, &task.recipient, &req.id)?;
            let out = json!({
                "type": "done", "id": req.id, "from": task.recipient, "ok": false, "error": "cancelled",
            });
            db::insert_item(&inner.db, me, "done", &req.id, &task.recipient, &out, ttl)?;
            inner.pump(me);
            info!(from = %me, id = %req.id, "cancelled while queued");
        } else {
            let out = json!({ "type": "cancel", "id": req.id, "from": me });
            db::insert_item(&inner.db, &task.recipient, "cancel", &req.id, me, &out, ttl)?;
            inner.pump(&task.recipient);
            info!(from = %me, to = %task.recipient, id = %req.id, "cancel forwarded");
        }
        Ok(())
    }

    fn ack(&self, me: &str, frame: Value) -> Handled {
        let req: AckIn = parse(frame)?;
        let item = match &req.item {
            Value::Number(n) => n.as_i64(),
            Value::String(s) => s.parse().ok(),
            _ => None,
        }
        .ok_or_else(|| reject("bad_frame", "item must be a number"))?;
        let inner = self.lock();
        db::ack(&inner.db, me, item)?;
        Ok(())
    }

    fn broadcast_memory(inner: &Inner, memory: &db::TeamMemory) {
        inner.broadcast(
            None,
            &json!({
                "type": "team_memory",
                "text": memory.text,
                "version": memory.version,
                "by": memory.updated_by,
            }),
        );
    }

    fn team_memory_append(&self, me: &str, frame: Value) -> Handled {
        let req: TextIn = parse(frame)?;
        let inner = self.lock();
        let current = db::team_memory(&inner.db)?;
        let mut text = current.text;
        if !text.is_empty() && !text.ends_with('\n') && !req.text.is_empty() {
            text.push('\n');
        }
        text.push_str(&req.text);
        let memory = db::set_team_memory(&inner.db, &text, me)?;
        Self::broadcast_memory(&inner, &memory);
        info!(by = %me, version = memory.version, "team memory appended");
        Ok(())
    }

    fn team_memory_set(&self, s: &Session, me: &str, frame: Value) -> Handled {
        let req: SetIn = parse(frame)?;
        let inner = self.lock();
        let current = db::team_memory(&inner.db)?;
        if current.version != req.version {
            send(
                &s.tx,
                error_frame(
                    None,
                    "version_conflict",
                    "the team memory changed; here is the latest",
                ),
            );
            send(
                &s.tx,
                json!({
                    "type": "team_memory",
                    "text": current.text,
                    "version": current.version,
                    "by": current.updated_by,
                }),
            );
            return Ok(());
        }
        let memory = db::set_team_memory(&inner.db, &req.text, me)?;
        Self::broadcast_memory(&inner, &memory);
        info!(by = %me, version = memory.version, "team memory set");
        Ok(())
    }

    fn leave(&self, s: &mut Session, me: &str) -> Handled {
        let ttl = Inner::ttl_ms(&self.cfg);
        let mut inner = self.lock();
        let senders = db::delete_account(&mut inner.db, me, ttl)?;
        inner.online.remove(me);
        s.account = None;
        inner.forget(me);
        for sender in senders {
            inner.pump(&sender);
        }
        send(&s.tx, json!({ "type": "member_left", "account": me }));
        let _ = s.tx.send(Out::Close);
        info!(account = %me, "left");
        Ok(())
    }

    /// Housekeeping, run every few seconds: notice kicks and renames made by
    /// the admin subcommands, deliver items they stored, and drop expired rows
    /// when `purge` is set.
    pub fn tick(&self, purge: bool) {
        let mut inner = self.lock();
        if purge {
            match db::purge(&inner.db, db::now_ms()) {
                Ok((0, 0)) => {}
                Ok((items, tasks)) => info!(items, tasks, "purged expired mailbox items"),
                Err(e) => warn!(error = %e, "purge failed"),
            }
        }
        let accounts = match db::accounts(&inner.db) {
            Ok(a) => a,
            Err(e) => {
                warn!(error = %e, "cannot read accounts");
                return;
            }
        };
        let present: HashMap<&str, &db::Account> =
            accounts.iter().map(|a| (a.id.as_str(), a)).collect();
        let gone: Vec<String> = inner
            .roster
            .keys()
            .filter(|id| !present.contains_key(id.as_str()))
            .cloned()
            .collect();
        for id in gone {
            if let Some(live) = inner.online.remove(&id) {
                send(
                    &live.tx,
                    error_frame(
                        None,
                        "account_gone",
                        "the router operator removed this account",
                    ),
                );
                let _ = live.tx.send(Out::Close);
            }
            inner.forget(&id);
            info!(account = %id, "account removed by admin");
        }
        for a in &accounts {
            if inner.roster.get(&a.id) != Some(&a.name) {
                inner.roster.insert(a.id.clone(), a.name.clone());
                inner.announce(&a.id);
            }
        }
        let online: Vec<String> = inner.online.keys().cloned().collect();
        for id in online {
            inner.pump(&id);
        }
    }

    /// Ids of accounts with a live connection.
    pub fn online(&self) -> Vec<String> {
        self.lock().online.keys().cloned().collect()
    }
}
