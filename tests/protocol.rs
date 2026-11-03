mod common;

use std::time::Duration;

use base64::Engine;
use common::{Options, Router};
use crew_router::hub::SignupMode;
use futures_util::SinkExt;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::Message;

const QUIET: Duration = Duration::from_millis(400);

#[tokio::test(flavor = "multi_thread")]
async fn health_and_hello() {
    let r = Router::start().await;
    let mut tcp = tokio::net::TcpStream::connect(r.addr).await.unwrap();
    tcp.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut body = String::new();
    tcp.read_to_string(&mut body).await.unwrap();
    let json_start = body.find('{').unwrap();
    let health: serde_json::Value = serde_json::from_str(&body[json_start..]).unwrap();
    assert_eq!(health["ok"], true);
    assert_eq!(health["name"], "crew-router");
    assert_eq!(health["protocol"], 1);
    assert_eq!(health["signup"], "open");
    assert!(health["version"].is_string());

    let mut c = r.connect().await;
    c.send(json!({ "type": "hello", "protocol": 1, "client": "test" }))
        .await;
    let s = c.recv().await;
    assert_eq!(s["type"], "server");
    assert_eq!(s["name"], "crew-router");
    assert_eq!(s["protocol"], 1);
    assert_eq!(s["title"], "test-router");
    assert_eq!(s["signup"], "open");

    c.send(json!({ "type": "ping" })).await;
    assert_eq!(c.recv().await["type"], "pong");
}

#[tokio::test(flavor = "multi_thread")]
async fn signup_names() {
    let r = Router::start().await;
    let (account, token) = r.signup("민지-bot_1").await;
    assert!(account.starts_with("acc_"));
    assert!(token.starts_with("crt_"));

    let mut c = r.connect().await;
    c.send(json!({ "type": "signup", "name": "민지-BOT_1" }))
        .await;
    let e = c.recv().await;
    assert_eq!(e["type"], "error");
    assert_eq!(e["code"], "name_taken");

    for bad in ["", "a b", "a/b", &"x".repeat(33)] {
        c.send(json!({ "type": "signup", "name": bad })).await;
        assert_eq!(c.recv().await["code"], "bad_name", "{bad:?}");
    }

    // The token is stored hashed only.
    let db = std::fs::read(r.data.join("crew-router.db")).unwrap();
    let needle = token.as_bytes();
    assert!(!db.windows(needle.len()).any(|w| w == needle));
}

#[tokio::test(flavor = "multi_thread")]
async fn auth_and_welcome() {
    let r = Router::start().await;
    let (a, token_a) = r.signup("alpha").await;
    let (b, token_b) = r.signup("beta").await;

    let mut c = r.connect().await;
    c.send(json!({ "type": "auth", "token": "crt_nope" })).await;
    assert_eq!(c.recv().await["code"], "bad_token");
    c.send(json!({ "type": "tell", "id": "t", "to": "beta", "text": "x", "hops": 0 }))
        .await;
    assert_eq!(c.recv().await["code"], "unauthorized");

    let (mut ca, w) = r.auth(&token_a).await;
    assert_eq!(w["account"], a);
    assert_eq!(w["name"], "alpha");
    assert_eq!(w["team_memory"], json!({ "text": "", "version": 0 }));
    let members = w["members"].as_array().unwrap();
    assert_eq!(members.len(), 1, "only teammates, not me: {w}");
    assert_eq!(members[0]["account"], b);
    assert_eq!(members[0]["online"], false);
    assert_eq!(members[0]["status"], "idle");

    let (mut cb, _) = r.auth(&token_b).await;
    let m = ca.expect("member").await;
    assert_eq!(m["member"]["account"], b);
    assert_eq!(m["member"]["online"], true);

    cb.send(json!({ "type": "presence", "status": "working" }))
        .await;
    let m = ca.expect("member").await;
    assert_eq!(m["member"]["status"], "working");

    cb.send(json!({ "type": "profile", "role": "reviewer", "avatar_color": "#ff5a5a" }))
        .await;
    let m = ca.expect("member").await;
    assert_eq!(m["member"]["role"], "reviewer");
    assert_eq!(m["member"]["avatar_color"], "#ff5a5a");

    cb.close().await;
    let m = ca.expect("member").await;
    assert_eq!(m["member"]["online"], false);
    assert_eq!(m["member"]["status"], "idle");
    assert_eq!(m["member"]["role"], "reviewer");
}

#[tokio::test(flavor = "multi_thread")]
async fn second_auth_replaces_first() {
    let r = Router::start().await;
    let (_, token) = r.signup("alpha").await;
    let (mut first, _) = r.auth(&token).await;
    let (mut second, _) = r.auth(&token).await;
    let e = first.recv().await;
    assert_eq!(e["code"], "replaced");
    assert!(first.closed().await);
    second.send(json!({ "type": "ping" })).await;
    assert_eq!(second.recv().await["type"], "pong");
}

#[tokio::test(flavor = "multi_thread")]
async fn tell_online() {
    let r = Router::start().await;
    let (mut ca, a, _) = r.join("alpha").await;
    let (mut cb, _, _) = r.join("beta").await;
    ca.send(json!({ "type": "tell", "id": "t1", "to": "BETA", "text": "hi", "hops": 1 }))
        .await;
    let acc = ca.expect("accepted").await;
    assert_eq!(
        acc,
        json!({ "type": "accepted", "id": "t1", "queued": false })
    );
    let t = cb.expect("tell").await;
    assert_eq!(t["id"], "t1");
    assert_eq!(t["from"], a);
    assert_eq!(t["from_name"], "alpha");
    assert_eq!(t["text"], "hi");
    assert_eq!(t["hops"], 1);
    assert!(t["item"].is_i64());
    assert!(t.get("files").is_none());

    ca.send(json!({ "type": "tell", "id": "t2", "to": "nobody", "text": "hi", "hops": 0 }))
        .await;
    let e = ca.expect("error").await;
    assert_eq!(e["code"], "no_such_member");
    assert_eq!(e["id"], "t2");
}

#[tokio::test(flavor = "multi_thread")]
async fn tell_offline_is_stored_until_acked() {
    let r = Router::start().await;
    let (mut ca, _, _) = r.join("alpha").await;
    let (_, token_b) = r.signup("beta").await;

    ca.send(json!({ "type": "tell", "id": "t1", "to": "beta", "text": "one", "hops": 0 }))
        .await;
    assert_eq!(ca.expect("accepted").await["queued"], true);
    ca.send(json!({ "type": "tell", "id": "t2", "to": "beta", "text": "two", "hops": 0 }))
        .await;
    assert_eq!(ca.expect("accepted").await["queued"], true);

    // Delivered right after welcome, in order.
    let (mut cb, _) = r.auth(&token_b).await;
    let first = cb.recv().await;
    let second = cb.recv().await;
    assert_eq!(first["type"], "tell");
    assert_eq!(first["text"], "one");
    assert_eq!(second["text"], "two");
    let item1 = first["item"].as_i64().unwrap();
    let item2 = second["item"].as_i64().unwrap();
    assert!(item1 < item2);

    // Ack only the first; the second comes back on reconnect with the same item.
    cb.send(json!({ "type": "ack", "item": item1 })).await;
    cb.send(json!({ "type": "ping" })).await;
    assert_eq!(cb.expect("pong").await["type"], "pong");
    cb.close().await;

    let (mut cb, _) = r.auth(&token_b).await;
    let again = cb.recv().await;
    assert_eq!(again["type"], "tell");
    assert_eq!(again["item"], item2);
    cb.send(json!({ "type": "ack", "item": item2 })).await;
    cb.expect_quiet(QUIET).await;
    cb.close().await;

    let (mut cb, _) = r.auth(&token_b).await;
    cb.expect_quiet(QUIET).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn replies_route_back_even_when_sender_is_offline() {
    let r = Router::start().await;
    let (mut ca, a, token_a) = r.join("alpha").await;
    let (mut cb, b, _) = r.join("beta").await;

    ca.send(json!({ "type": "tell", "id": "t1", "to": "beta", "text": "go", "hops": 0 }))
        .await;
    ca.expect("accepted").await;
    let t = cb.expect("tell").await;
    cb.send(json!({ "type": "ack", "item": t["item"] })).await;

    // Online: progress and reply arrive live.
    cb.send(json!({ "type": "progress", "id": "t1", "state": "started" }))
        .await;
    let p = ca.expect("progress").await;
    assert_eq!(
        p,
        json!({ "type": "progress", "id": "t1", "from": b, "state": "started" })
    );
    cb.send(json!({ "type": "reply", "id": "t1", "text": "first" }))
        .await;
    let rep = ca.expect("reply").await;
    assert_eq!(rep["from"], b);
    assert_eq!(rep["text"], "first");
    ca.send(json!({ "type": "ack", "item": rep["item"] })).await;

    // Offline: progress is dropped, reply and done wait.
    ca.close().await;
    cb.send(json!({ "type": "progress", "id": "t1", "state": "working", "text": "sec" }))
        .await;
    cb.send(json!({ "type": "reply", "id": "t1", "text": "second" }))
        .await;
    cb.send(json!({ "type": "done", "id": "t1", "ok": true }))
        .await;
    cb.send(json!({ "type": "ping" })).await;
    cb.expect("pong").await;

    let (mut ca, _) = r.auth(&token_a).await;
    let rep = ca.recv().await;
    assert_eq!(rep["type"], "reply");
    assert_eq!(rep["text"], "second");
    let done = ca.recv().await;
    assert_eq!(done["type"], "done");
    assert_eq!(done["id"], "t1");
    assert_eq!(done["from"], b);
    assert_eq!(done["ok"], true);
    assert!(done.get("error").is_none());
    assert!(done["item"].is_i64());
    ca.expect_quiet(QUIET).await;

    // The route is closed after done.
    cb.send(json!({ "type": "reply", "id": "t1", "text": "late" }))
        .await;
    let e = cb.expect("error").await;
    assert_eq!(e["code"], "unknown_id");
    let _ = a;
}

#[tokio::test(flavor = "multi_thread")]
async fn routes_survive_a_restart() {
    let r = Router::start().await;
    let (mut ca, _, token_a) = r.join("alpha").await;
    let (_, token_b) = r.signup("beta").await;
    ca.send(json!({ "type": "tell", "id": "t1", "to": "beta", "text": "go", "hops": 0 }))
        .await;
    ca.expect("accepted").await;
    ca.close().await;
    let data = r.data.clone();
    let dir = r.stop().await;

    let r = Router::on(&data, Options::default()).await;
    let (mut cb, _) = r.auth(&token_b).await;
    let t = cb.recv().await;
    assert_eq!(t["type"], "tell");
    cb.send(json!({ "type": "done", "id": "t1", "ok": false, "error": "failed" }))
        .await;
    let (mut ca, _) = r.auth(&token_a).await;
    let done = ca.recv().await;
    assert_eq!(done["type"], "done");
    assert_eq!(done["error"], "failed");
    drop(dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_while_queued_and_after_delivery() {
    let r = Router::start().await;
    let (mut ca, _, _) = r.join("alpha").await;
    let (b, token_b) = r.signup("beta").await;

    ca.send(json!({ "type": "tell", "id": "t1", "to": "beta", "text": "go", "hops": 0 }))
        .await;
    ca.expect("accepted").await;
    ca.send(json!({ "type": "cancel", "id": "t1" })).await;
    let done = ca.expect("done").await;
    assert_eq!(done["id"], "t1");
    assert_eq!(done["from"], b);
    assert_eq!(done["ok"], false);
    assert_eq!(done["error"], "cancelled");

    let (mut cb, _) = r.auth(&token_b).await;
    cb.expect_quiet(QUIET).await;

    // Once delivered, the cancel goes to the recipient instead.
    ca.send(json!({ "type": "tell", "id": "t2", "to": "beta", "text": "go", "hops": 0 }))
        .await;
    ca.expect("accepted").await;
    cb.expect("tell").await;
    ca.send(json!({ "type": "cancel", "id": "t2" })).await;
    let c = cb.expect("cancel").await;
    assert_eq!(c["id"], "t2");
    assert!(c["item"].is_i64());
    ca.expect_quiet(QUIET).await;

    ca.send(json!({ "type": "cancel", "id": "nope" })).await;
    assert_eq!(ca.expect("error").await["code"], "unknown_id");
}

#[tokio::test(flavor = "multi_thread")]
async fn team_memory_versions() {
    let r = Router::start().await;
    let (mut ca, a, _) = r.join("alpha").await;
    let (mut cb, _, _) = r.join("beta").await;

    ca.send(json!({ "type": "team_memory_append", "text": "- ship friday" }))
        .await;
    let m = cb.expect("team_memory").await;
    assert_eq!(m["text"], "- ship friday");
    assert_eq!(m["version"], 1);
    assert_eq!(m["by"], a);
    assert_eq!(ca.expect("team_memory").await["version"], 1);

    ca.send(json!({ "type": "team_memory_append", "text": "- no deploys monday" }))
        .await;
    assert_eq!(
        ca.expect("team_memory").await["text"],
        "- ship friday\n- no deploys monday"
    );
    cb.expect("team_memory").await;

    cb.send(json!({ "type": "team_memory_set", "text": "stale", "version": 1 }))
        .await;
    let e = cb.expect("error").await;
    assert_eq!(e["code"], "version_conflict");
    let latest = cb.expect("team_memory").await;
    assert_eq!(latest["version"], 2);
    ca.expect_quiet(QUIET).await;

    cb.send(json!({ "type": "team_memory_set", "text": "fresh", "version": 2 }))
        .await;
    assert_eq!(cb.expect("team_memory").await["version"], 3);
    assert_eq!(ca.expect("team_memory").await["text"], "fresh");
}

#[tokio::test(flavor = "multi_thread")]
async fn leave_frees_the_name_and_fails_pending_tells() {
    let r = Router::start().await;
    let (mut ca, _, token_a) = r.join("alpha").await;
    let (mut cb, b, _) = r.join("beta").await;

    ca.send(json!({ "type": "tell", "id": "t1", "to": "beta", "text": "go", "hops": 0 }))
        .await;
    ca.expect("accepted").await;
    cb.expect("tell").await;
    ca.close().await;

    cb.send(json!({ "type": "leave" })).await;
    let left = cb.expect("member_left").await;
    assert_eq!(left["account"], b);
    assert!(cb.closed().await);

    let (mut ca, w) = r.auth(&token_a).await;
    assert_eq!(w["members"], json!([]));
    let done = ca.recv().await;
    assert_eq!(done["type"], "done");
    assert_eq!(done["id"], "t1");
    assert_eq!(done["from"], b);
    assert_eq!(done["ok"], false);
    assert_eq!(done["error"], "recipient_gone");

    // The name is free again, as a new account.
    let (b2, _) = r.signup("Beta").await;
    assert_ne!(b2, b);
    let m = ca.expect("member").await;
    assert_eq!(m["member"]["account"], b2);
}

#[tokio::test(flavor = "multi_thread")]
async fn leave_while_others_watch() {
    let r = Router::start().await;
    let (mut ca, _, _) = r.join("alpha").await;
    let (mut cb, b, _) = r.join("beta").await;
    cb.send(json!({ "type": "leave" })).await;
    let left = ca.expect("member_left").await;
    assert_eq!(left["account"], b);
}

#[tokio::test(flavor = "multi_thread")]
async fn rate_limit() {
    let r = Router::start().await;
    let (mut ca, _, _) = r.join("alpha").await;
    let (_, _) = r.signup("beta").await;
    for i in 0..60 {
        ca.send(
            json!({ "type": "tell", "id": format!("t{i}"), "to": "beta", "text": "x", "hops": 0 }),
        )
        .await;
        assert_eq!(ca.expect("accepted").await["id"], format!("t{i}"));
    }
    ca.send(json!({ "type": "tell", "id": "t60", "to": "beta", "text": "x", "hops": 0 }))
        .await;
    let e = ca.expect("error").await;
    assert_eq!(e["code"], "rate_limited");
    assert_eq!(e["id"], "t60");

    // A resend of an accepted id is not a new tell.
    ca.send(json!({ "type": "tell", "id": "t0", "to": "beta", "text": "x", "hops": 0 }))
        .await;
    assert_eq!(ca.expect("accepted").await["queued"], true);
}

#[tokio::test(flavor = "multi_thread")]
async fn files_over_25mb_are_refused() {
    let r = Router::start().await;
    let (mut ca, _, _) = r.join("alpha").await;
    let (mut cb, _, _) = r.join("beta").await;

    let small = base64::engine::general_purpose::STANDARD.encode(b"hello");
    ca.send(json!({
        "type": "tell", "id": "t1", "to": "beta", "text": "file", "hops": 0,
        "files": [{ "name": "a.txt", "size": 5, "sha256": "x", "data": small }],
    }))
    .await;
    ca.expect("accepted").await;
    let t = cb.expect("tell").await;
    assert_eq!(t["files"][0]["name"], "a.txt");
    assert_eq!(t["files"][0]["data"], small);

    // A 25MB+ file cannot fit a 32MB frame once base64'd, so the declared
    // size is what trips the limit in practice.
    ca.send(json!({
        "type": "tell", "id": "t2", "to": "beta", "text": "file", "hops": 0,
        "files": [{ "name": "big.bin", "size": 26 * 1024 * 1024, "sha256": "x", "data": small }],
    }))
    .await;
    let e = ca.expect("error").await;
    assert_eq!(e["code"], "too_large");
    assert_eq!(e["id"], "t2");
}

#[tokio::test(flavor = "multi_thread")]
async fn invite_and_closed_signup() {
    let r = Router::with(Options {
        signup: SignupMode::Invite,
        invite_code: Some("letmein".into()),
        ..Options::default()
    })
    .await;
    let mut c = r.connect().await;
    c.send(json!({ "type": "hello", "protocol": 1, "client": "test" }))
        .await;
    assert_eq!(c.recv().await["signup"], "invite");
    c.send(json!({ "type": "signup", "name": "alpha" })).await;
    assert_eq!(c.recv().await["code"], "invite_required");
    c.send(json!({ "type": "signup", "name": "alpha", "invite": "wrong" }))
        .await;
    assert_eq!(c.recv().await["code"], "invite_required");
    c.send(json!({ "type": "signup", "name": "alpha", "invite": "letmein" }))
        .await;
    assert_eq!(c.recv().await["type"], "signed_up");

    let r = Router::with(Options {
        signup: SignupMode::Closed,
        ..Options::default()
    })
    .await;
    let mut c = r.connect().await;
    c.send(json!({ "type": "signup", "name": "alpha" })).await;
    assert_eq!(c.recv().await["code"], "signup_closed");
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_kick_and_rename_reach_live_members() {
    let r = Router::start().await;
    let (mut ca, a, _) = r.join("alpha").await;
    let (mut cb, b, _) = r.join("beta").await;
    crew_router::admin::users_rename(&r.data, "beta", "gamma").unwrap();
    loop {
        // Beta's signup and auth announced it too; wait for the rename.
        let m = ca.expect("member").await;
        assert_eq!(m["member"]["account"], b);
        if m["member"]["name"] == "gamma" {
            break;
        }
    }

    ca.send(json!({ "type": "tell", "id": "t1", "to": "gamma", "text": "go", "hops": 0 }))
        .await;
    ca.expect("accepted").await;
    cb.expect("tell").await;

    crew_router::admin::users_kick(&r.data, "gamma", Duration::from_secs(3600)).unwrap();
    let e = cb.expect("error").await;
    assert_eq!(e["code"], "account_gone");
    assert!(cb.closed().await);
    let left = ca.expect("member_left").await;
    assert_eq!(left["account"], b);
    let done = ca.expect("done").await;
    assert_eq!(done["error"], "recipient_gone");
    let _ = a;

    let list = crew_router::admin::users_list(&r.data).unwrap();
    assert!(list.contains("alpha"));
    assert!(!list.contains("gamma"));
    let stats = crew_router::admin::stats(&r.data).unwrap();
    assert!(stats.contains("accounts        1"), "{stats}");
}

#[tokio::test(flavor = "multi_thread")]
async fn expired_items_are_purged() {
    let r = Router::with(Options {
        ttl: Duration::from_millis(300),
        purge_every: Duration::from_millis(200),
        ..Options::default()
    })
    .await;
    let (mut ca, _, _) = r.join("alpha").await;
    let (_, token_b) = r.signup("beta").await;
    ca.send(json!({ "type": "tell", "id": "t1", "to": "beta", "text": "go", "hops": 0 }))
        .await;
    ca.expect("accepted").await;
    let done = ca.expect("done").await;
    assert_eq!(done["error"], "expired");
    ca.send(json!({ "type": "ack", "item": done["item"] }))
        .await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let (mut cb, _) = r.auth(&token_b).await;
    cb.expect_quiet(QUIET).await;
    let stats = crew_router::admin::stats(&r.data).unwrap();
    assert!(stats.contains("mailbox items   0"), "{stats}");
    assert!(stats.contains("open tells      0"), "{stats}");
}

#[tokio::test(flavor = "multi_thread")]
async fn expired_tell_comes_back_to_its_sender() {
    let r = Router::with(Options {
        ttl: Duration::from_millis(1000),
        purge_every: Duration::from_millis(100),
        ..Options::default()
    })
    .await;
    let (mut ca, _, _) = r.join("alpha").await;
    let (b, token_b) = r.signup("beta").await;
    ca.send(json!({ "type": "tell", "id": "t1", "to": "beta", "text": "go", "hops": 0 }))
        .await;
    assert_eq!(ca.expect("accepted").await["queued"], true);

    let done = ca.expect("done").await;
    assert_eq!(done["id"], "t1");
    assert_eq!(done["from"], b);
    assert_eq!(done["ok"], false);
    assert_eq!(done["error"], "expired");
    assert!(done["item"].is_i64());
    ca.send(json!({ "type": "ack", "item": done["item"] }))
        .await;

    // The route is closed, and the recipient never sees the tell.
    ca.send(json!({ "type": "cancel", "id": "t1" })).await;
    assert_eq!(ca.expect("error").await["code"], "unknown_id");
    let (mut cb, _) = r.auth(&token_b).await;
    cb.expect_quiet(QUIET).await;
    ca.expect_quiet(QUIET).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn expired_tell_waits_for_an_offline_sender() {
    let opts = || Options {
        ttl: Duration::from_millis(2000),
        purge_every: Duration::from_millis(100),
        ..Options::default()
    };
    let r = Router::with(opts()).await;
    let (mut ca, a, token_a) = r.join("alpha").await;
    let (b, token_b) = r.signup("beta").await;
    ca.send(json!({ "type": "tell", "id": "t1", "to": "beta", "text": "go", "hops": 0 }))
        .await;
    ca.expect("accepted").await;
    ca.close().await;

    // Wait until the tell has turned into a notice in alpha's mailbox.
    let deadline = tokio::time::Instant::now() + common::WAIT;
    loop {
        let conn = crew_router::db::open(&r.data).unwrap();
        let mine = crew_router::db::mailbox_count(&conn, &a).unwrap();
        let theirs = crew_router::db::mailbox_count(&conn, &b).unwrap();
        if mine == 1 && theirs == 0 {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "no notice stored");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // It is stored, so it outlives a restart.
    let data = r.data.clone();
    let dir = r.stop().await;
    let r = Router::on(&data, opts()).await;

    let (mut ca, _) = r.auth(&token_a).await;
    let done = ca.recv().await;
    assert_eq!(done["type"], "done");
    assert_eq!(done["id"], "t1");
    assert_eq!(done["from"], b);
    assert_eq!(done["ok"], false);
    assert_eq!(done["error"], "expired");
    assert!(done["item"].is_i64());
    ca.send(json!({ "type": "ack", "item": done["item"] }))
        .await;
    ca.expect_quiet(QUIET).await;

    let (mut cb, _) = r.auth(&token_b).await;
    cb.expect_quiet(QUIET).await;
    drop(dir);
}

/// Kills the child router when a test ends, pass or fail.
struct Child(std::process::Child);

impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn ttl_secs_flag_expires_tells_in_seconds() {
    let bin = env!("CARGO_BIN_EXE_crew-router");
    let help = std::process::Command::new(bin)
        .args(["serve", "--help"])
        .output()
        .unwrap();
    let help = String::from_utf8_lossy(&help.stdout);
    assert!(help.contains("--ttl-days"), "{help}");
    assert!(!help.contains("ttl-secs"), "the flag is hidden: {help}");

    let dir = tempfile::tempdir().unwrap();
    let addr = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let _child = Child(
        std::process::Command::new(bin)
            .arg("--data")
            .arg(dir.path())
            .args(["serve", "--listen", &addr.to_string(), "--ttl-secs", "1"])
            .env("CREW_ROUTER_LOG", "error")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    let url = format!("ws://{addr}/ws");
    let deadline = tokio::time::Instant::now() + common::WAIT;
    let connect = || async {
        loop {
            match tokio_tungstenite::connect_async(&url).await {
                Ok((ws, _)) => return common::Client { ws },
                Err(e) => assert!(tokio::time::Instant::now() < deadline, "{e}"),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };

    let mut c = connect().await;
    c.send(json!({ "type": "signup", "name": "beta" })).await;
    let b = c.expect("signed_up").await["account"].clone();
    c.send(json!({ "type": "signup", "name": "alpha" })).await;
    let token = c.expect("signed_up").await["token"].clone();
    let mut ca = connect().await;
    ca.send(json!({ "type": "auth", "token": token })).await;
    ca.expect("welcome").await;
    ca.send(json!({ "type": "tell", "id": "t1", "to": "beta", "text": "go", "hops": 0 }))
        .await;
    assert_eq!(ca.expect("accepted").await["queued"], true);
    let done = ca.expect("done").await;
    assert_eq!(done["id"], "t1");
    assert_eq!(done["from"], b);
    assert_eq!(done["error"], "expired");
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_frames_get_errors() {
    let r = Router::start().await;
    let (mut ca, _, _) = r.join("alpha").await;
    ca.send(json!({ "type": "wat" })).await;
    assert_eq!(ca.expect("error").await["code"], "bad_frame");
    ca.send(json!({ "type": "tell", "id": "t1" })).await;
    let e = ca.expect("error").await;
    assert_eq!(e["code"], "bad_frame");
    assert_eq!(e["id"], "t1");
    ca.send(json!({ "type": "presence", "status": "sleeping" }))
        .await;
    assert_eq!(ca.expect("error").await["code"], "bad_frame");
    ca.send(json!({ "type": "auth", "token": "x" })).await;
    assert_eq!(ca.expect("error").await["code"], "bad_frame");
}

#[tokio::test(flavor = "multi_thread")]
async fn frames_over_32mb_close_the_connection() {
    let r = Router::start().await;
    let (mut ca, _, token) = r.join("alpha").await;
    let huge = "x".repeat(33 * 1024 * 1024);
    let frame = json!({ "type": "team_memory_append", "text": huge }).to_string();
    // The router may hang up before the client finishes writing.
    let sent = ca.ws.send(Message::Text(frame.into())).await;
    assert!(sent.is_err() || ca.closed().await);
    // The router itself is fine.
    let (mut again, _) = r.auth(&token).await;
    again.send(json!({ "type": "ping" })).await;
    assert_eq!(again.expect("pong").await["type"], "pong");
}
