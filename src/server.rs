//! HTTP and WebSocket front: `GET /health` and `/ws`.

use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, State};
use axum::response::{IntoResponse, Json};
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::sync::mpsc;
use tracing::{debug, info};

use crate::db;
use crate::hub::{Hub, HubConfig, Out, MAX_FRAME_BYTES, PROTOCOL, VERSION};

/// The router pings an idle client this often…
const PING_EVERY: Duration = Duration::from_secs(30);
/// …and drops it when nothing at all arrives for this long.
const IDLE_TIMEOUT: Duration = Duration::from_secs(90);

pub struct ServeConfig {
    pub data_dir: PathBuf,
    pub hub: HubConfig,
    pub tls: Option<(PathBuf, PathBuf)>,
    /// How often to look for admin changes and new mailbox items.
    pub tick: Duration,
    /// How often to drop expired rows.
    pub purge_every: Duration,
}

impl ServeConfig {
    pub fn new(data_dir: PathBuf, hub: HubConfig) -> Self {
        ServeConfig {
            data_dir,
            hub,
            tls: None,
            tick: Duration::from_secs(5),
            purge_every: Duration::from_secs(60),
        }
    }
}

pub fn app(hub: Arc<Hub>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ws", get(ws))
        .with_state(hub)
}

async fn health(State(hub): State<Arc<Hub>>) -> impl IntoResponse {
    Json(json!({
        "ok": true,
        "name": "crew-router",
        "version": VERSION,
        "protocol": PROTOCOL,
        "signup": hub.config().signup.as_str(),
    }))
}

async fn ws(
    State(hub): State<Arc<Hub>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    upgrade: WebSocketUpgrade,
) -> impl IntoResponse {
    upgrade
        .max_message_size(MAX_FRAME_BYTES)
        .max_frame_size(MAX_FRAME_BYTES)
        .on_upgrade(move |socket| connection(hub, socket, peer))
}

async fn connection(hub: Arc<Hub>, socket: WebSocket, peer: SocketAddr) {
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<Out>();
    let mut session = hub.open_session(tx);
    info!(conn = session.conn, %peer, "connected");

    let mut writer = tokio::spawn(async move {
        let mut ping = tokio::time::interval(PING_EVERY);
        ping.tick().await;
        loop {
            tokio::select! {
                out = rx.recv() => match out {
                    Some(Out::Frame(text)) => {
                        if sink.send(Message::Text(text.into())).await.is_err() {
                            break;
                        }
                    }
                    Some(Out::Close) | None => {
                        let _ = sink.send(Message::Close(None)).await;
                        break;
                    }
                },
                _ = ping.tick() => {
                    if sink.send(Message::Ping(Vec::new().into())).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    loop {
        tokio::select! {
            _ = &mut writer => break,
            next = tokio::time::timeout(IDLE_TIMEOUT, stream.next()) => {
                let msg = match next {
                    Err(_) => {
                        debug!(conn = session.conn, "idle timeout");
                        break;
                    }
                    Ok(None) | Ok(Some(Err(_))) => break,
                    Ok(Some(Ok(msg))) => msg,
                };
                match msg {
                    Message::Text(text) => {
                        // Storage is synchronous SQLite; keep it off the other tasks' toes.
                        let keep = tokio::task::block_in_place(|| hub.handle(&mut session, text.as_str()));
                        if !keep {
                            // The writer sends the close frame; wait for it.
                            let _ = tokio::time::timeout(Duration::from_secs(5), &mut writer).await;
                            break;
                        }
                    }
                    Message::Binary(_) => {
                        let error = json!({
                            "type": "error",
                            "code": "bad_frame",
                            "message": "frames are JSON text",
                        });
                        let _ = session.tx.send(Out::Frame(error.to_string()));
                    }
                    Message::Close(_) => break,
                    Message::Ping(_) | Message::Pong(_) => {}
                }
            }
        }
    }
    hub.disconnect(&session);
    writer.abort();
    info!(conn = session.conn, "disconnected");
}

/// Runs the router on `listener` until `shutdown` resolves.
pub async fn run(
    listener: std::net::TcpListener,
    cfg: ServeConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let conn = db::open(&cfg.data_dir)?;
    let hub = Arc::new(Hub::new(cfg.hub.clone(), conn)?);

    let ticker = {
        let hub = hub.clone();
        let tick = cfg.tick;
        let purge_every = cfg.purge_every;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tick);
            let mut last_purge = tokio::time::Instant::now();
            loop {
                interval.tick().await;
                let purge = last_purge.elapsed() >= purge_every;
                if purge {
                    last_purge = tokio::time::Instant::now();
                }
                let hub = hub.clone();
                let _ = tokio::task::spawn_blocking(move || hub.tick(purge)).await;
            }
        })
    };

    let handle = axum_server::Handle::new();
    {
        let handle = handle.clone();
        tokio::spawn(async move {
            shutdown.await;
            info!("shutting down");
            handle.graceful_shutdown(Some(Duration::from_secs(5)));
        });
    }

    listener.set_nonblocking(true)?;
    let addr = listener.local_addr()?;
    let service = app(hub).into_make_service_with_connect_info::<SocketAddr>();
    let result = match &cfg.tls {
        Some((cert, key)) => {
            let _ = rustls::crypto::ring::default_provider().install_default();
            let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key)
                .await
                .with_context(|| {
                    format!(
                        "cannot load TLS cert {} / key {}",
                        cert.display(),
                        key.display()
                    )
                })?;
            info!(%addr, "listening (wss)");
            axum_server::from_tcp_rustls(listener, tls)?
                .handle(handle)
                .serve(service)
                .await
        }
        None => {
            info!(%addr, "listening (ws, no TLS)");
            axum_server::from_tcp(listener)?
                .handle(handle)
                .serve(service)
                .await
        }
    };
    ticker.abort();
    result.context("server error")
}
