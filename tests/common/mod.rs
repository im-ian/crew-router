#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crew_router::hub::{HubConfig, SignupMode};
use crew_router::server::{self, ServeConfig};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub const WAIT: Duration = Duration::from_secs(5);

pub struct Router {
    pub addr: SocketAddr,
    pub data: PathBuf,
    stop: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
    _dir: Option<tempfile::TempDir>,
}

pub struct Options {
    pub signup: SignupMode,
    pub invite_code: Option<String>,
    pub ttl: Duration,
    pub tick: Duration,
    pub purge_every: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            signup: SignupMode::Open,
            invite_code: None,
            ttl: Duration::from_secs(30 * 86_400),
            tick: Duration::from_millis(100),
            purge_every: Duration::from_secs(60),
        }
    }
}

impl Router {
    pub async fn start() -> Router {
        Router::with(Options::default()).await
    }

    pub async fn with(opts: Options) -> Router {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().to_path_buf();
        let mut r = Router::on(&data, opts).await;
        r._dir = Some(dir);
        r
    }

    /// Starts a router on an existing data dir.
    pub async fn on(data: &Path, opts: Options) -> Router {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let hub = HubConfig {
            title: "test-router".into(),
            signup: opts.signup,
            invite_code: opts.invite_code,
            ttl: opts.ttl,
        };
        let mut cfg = ServeConfig::new(data.to_path_buf(), hub);
        cfg.tick = opts.tick;
        cfg.purge_every = opts.purge_every;
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            server::run(listener, cfg, async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
        });
        Router {
            addr,
            data: data.to_path_buf(),
            stop: Some(stop),
            task: Some(task),
            _dir: None,
        }
    }

    /// Stops the server and keeps the data dir. Returns the temp dir guard.
    pub async fn stop(mut self) -> Option<tempfile::TempDir> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = tokio::time::timeout(Duration::from_secs(10), task).await;
        }
        self._dir.take()
    }

    pub fn url(&self) -> String {
        format!("ws://{}/ws", self.addr)
    }

    pub async fn connect(&self) -> Client {
        let (ws, _) = tokio_tungstenite::connect_async(self.url()).await.unwrap();
        Client { ws }
    }

    /// Signs up `name` and returns (account, token).
    pub async fn signup(&self, name: &str) -> (String, String) {
        let mut c = self.connect().await;
        c.send(json!({ "type": "signup", "name": name })).await;
        let r = c.recv().await;
        assert_eq!(r["type"], "signed_up", "{r}");
        (
            r["account"].as_str().unwrap().to_string(),
            r["token"].as_str().unwrap().to_string(),
        )
    }

    /// Connects and authenticates. Returns the client and the welcome frame.
    pub async fn auth(&self, token: &str) -> (Client, Value) {
        let mut c = self.connect().await;
        c.send(json!({ "type": "auth", "token": token })).await;
        let w = c.recv().await;
        assert_eq!(w["type"], "welcome", "{w}");
        (c, w)
    }

    /// Signs up and connects in one go: (client, account, token).
    pub async fn join(&self, name: &str) -> (Client, String, String) {
        let (account, token) = self.signup(name).await;
        let (c, _) = self.auth(&token).await;
        (c, account, token)
    }
}

impl Drop for Router {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

pub struct Client {
    pub ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl Client {
    pub async fn send(&mut self, v: Value) {
        self.ws
            .send(Message::Text(v.to_string().into()))
            .await
            .unwrap();
    }

    /// Next JSON frame. Panics after five seconds.
    pub async fn recv(&mut self) -> Value {
        self.try_recv(WAIT)
            .await
            .expect("no frame within five seconds")
    }

    /// Next JSON frame within `wait`, or None. Also None once the socket closes.
    pub async fn try_recv(&mut self, wait: Duration) -> Option<Value> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let next = tokio::time::timeout_at(deadline, self.ws.next())
                .await
                .ok()??;
            match next {
                Ok(Message::Text(t)) => return Some(serde_json::from_str(t.as_str()).unwrap()),
                Ok(Message::Close(_)) | Err(_) => return None,
                Ok(_) => continue,
            }
        }
    }

    /// The next frame of type `kind`, skipping `member`/`members` roster noise.
    pub async fn expect(&mut self, kind: &str) -> Value {
        loop {
            let f = self.recv().await;
            if f["type"] == kind {
                return f;
            }
            assert!(
                f["type"] == "member" || f["type"] == "members",
                "wanted {kind}, got {f}"
            );
        }
    }

    /// Asserts nothing but roster frames arrives within `wait`.
    pub async fn expect_quiet(&mut self, wait: Duration) {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() {
                return;
            }
            match self.try_recv(left).await {
                None => return,
                Some(f) if f["type"] == "member" || f["type"] == "members" => continue,
                Some(f) => panic!("expected nothing, got {f}"),
            }
        }
    }

    /// True once the server closes the socket.
    pub async fn closed(&mut self) -> bool {
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            match tokio::time::timeout_at(deadline, self.ws.next()).await {
                Err(_) => return false,
                Ok(None) | Ok(Some(Err(_))) | Ok(Some(Ok(Message::Close(_)))) => return true,
                Ok(Some(Ok(_))) => continue,
            }
        }
    }

    pub async fn close(mut self) {
        let _ = self.ws.close(None).await;
        // Let the server notice before the next step of a test.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
