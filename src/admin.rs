//! `users` and `stats`: operator commands that work on the data dir directly.
//!
//! They can run while the server is up. The server notices kicks and renames
//! on its next tick (a few seconds) and tells the members.

use std::fmt::Write as _;
use std::path::Path;
use std::time::Duration;

use anyhow::{bail, Result};

use crate::db;
use crate::ids;

/// Lists accounts as a table.
pub fn users_list(data: &Path) -> Result<String> {
    let conn = db::open(data)?;
    let accounts = db::accounts(&conn)?;
    let counts = db::mailbox_counts(&conn)?;
    let mut out = String::new();
    let width = accounts
        .iter()
        .map(|a| a.name.chars().count())
        .max()
        .unwrap_or(0)
        .max(4);
    let _ = writeln!(
        out,
        "{:<width$}  {:<24}  {:<20}  {:<20}  MAILBOX",
        "NAME", "ACCOUNT", "CREATED (UTC)", "LAST SEEN (UTC)"
    );
    for a in &accounts {
        let pad = width - a.name.chars().count();
        let _ = writeln!(
            out,
            "{}{}  {:<24}  {:<20}  {:<20}  {}",
            a.name,
            " ".repeat(pad),
            a.id,
            format_time(a.created_at),
            a.last_seen.map(format_time).unwrap_or_else(|| "-".into()),
            counts.get(&a.id).copied().unwrap_or(0),
        );
    }
    let _ = write!(out, "{} account(s)", accounts.len());
    Ok(out)
}

/// Deletes an account the way `leave` does.
pub fn users_kick(data: &Path, name: &str, ttl: Duration) -> Result<String> {
    let mut conn = db::open(data)?;
    let Some(account) = db::account_by_name_key(&conn, &ids::name_key(name))? else {
        bail!("no account is named {name}");
    };
    let ttl_ms = ttl.as_millis().min(i64::MAX as u128) as i64;
    let senders = db::delete_account(&mut conn, &account.id, ttl_ms)?;
    Ok(format!(
        "kicked {} ({}). {} sender(s) will get recipient_gone.",
        account.name,
        account.id,
        senders.len()
    ))
}

pub fn users_rename(data: &Path, old: &str, new: &str) -> Result<String> {
    if !ids::valid_name(new) {
        bail!("{new} is not a valid name: 1–32 letters, digits, `-` or `_`");
    }
    let conn = db::open(data)?;
    let Some(account) = db::account_by_name_key(&conn, &ids::name_key(old))? else {
        bail!("no account is named {old}");
    };
    let key = ids::name_key(new);
    if let Some(other) = db::account_by_name_key(&conn, &key)? {
        if other.id != account.id {
            bail!("{new} is taken by {}", other.id);
        }
    }
    db::rename(&conn, &account.id, new, &key)?;
    Ok(format!(
        "renamed {} to {new} ({})",
        account.name, account.id
    ))
}

pub fn stats(data: &Path) -> Result<String> {
    let conn = db::open(data)?;
    let s = db::stats(&conn)?;
    let file = std::fs::metadata(data.join(db::DB_FILE))
        .map(|m| m.len())
        .unwrap_or(0);
    Ok(format!(
        "accounts        {}\nmailbox items   {}\nmailbox size    {}\nopen tells      {}\nteam memory     v{}, {}\ndatabase        {}",
        s.accounts,
        s.mailbox_items,
        human_bytes(s.mailbox_bytes.max(0) as u64),
        s.open_tells,
        s.team_memory_version,
        human_bytes(s.team_memory_bytes.max(0) as u64),
        human_bytes(file),
    ))
}

fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];
    let mut v = n as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit < UNITS.len() - 1 {
        v /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[unit])
    }
}

/// `YYYY-MM-DD HH:MM:SS` in UTC from Unix milliseconds.
pub fn format_time(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times() {
        assert_eq!(format_time(0), "1970-01-01 00:00:00");
        assert_eq!(format_time(1_762_128_000_000), "2025-11-03 00:00:00");
        assert_eq!(format_time(951_782_400_000), "2000-02-29 00:00:00");
    }

    #[test]
    fn bytes() {
        assert_eq!(human_bytes(10), "10 B");
        assert_eq!(human_bytes(2048), "2.0 KB");
        assert_eq!(human_bytes(25 * 1024 * 1024), "25.0 MB");
    }
}
