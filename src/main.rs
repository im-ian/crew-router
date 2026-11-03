use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Args, Parser, Subcommand, ValueEnum};
use tracing_subscriber::EnvFilter;

use crew_router::admin;
use crew_router::hub::{HubConfig, SignupMode};
use crew_router::server::{self, ServeConfig};

/// Relay server for Crew remote bots.
#[derive(Parser)]
#[command(name = "crew-router", version, about)]
struct Cli {
    /// Directory for the database.
    #[arg(
        long,
        global = true,
        env = "CREW_ROUTER_DATA",
        default_value = "./data"
    )]
    data: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the router.
    Serve(ServeArgs),
    /// Look at or change accounts.
    Users {
        #[command(subcommand)]
        command: UsersCommand,
    },
    /// Count accounts and mailbox items.
    Stats,
}

#[derive(Subcommand)]
enum UsersCommand {
    /// List accounts.
    List,
    /// Delete an account. Pending tells to it fail as recipient_gone.
    Kick {
        name: String,
        /// How long the resulting failure notices are kept.
        #[arg(long, env = "CREW_ROUTER_TTL_DAYS", default_value_t = 30)]
        ttl_days: u32,
    },
    /// Change an account's name.
    Rename { old: String, new: String },
}

#[derive(Clone, Copy, ValueEnum)]
enum Signup {
    Open,
    Invite,
    Closed,
}

#[derive(Args)]
struct ServeArgs {
    /// Address and port to listen on.
    #[arg(long, env = "CREW_ROUTER_LISTEN", default_value = "0.0.0.0:7420")]
    listen: SocketAddr,

    /// Name shown to Crew when someone adds this router. Defaults to the host name.
    #[arg(long, env = "CREW_ROUTER_TITLE")]
    title: Option<String>,

    /// Who may sign up.
    #[arg(long, env = "CREW_ROUTER_SIGNUP", value_enum, default_value = "open")]
    signup: Signup,

    /// Code new members must give when --signup invite.
    #[arg(long, env = "CREW_ROUTER_INVITE_CODE", hide_env_values = true)]
    invite_code: Option<String>,

    /// Days to keep undelivered messages.
    #[arg(long, env = "CREW_ROUTER_TTL_DAYS", default_value_t = 30)]
    ttl_days: u32,

    /// PEM certificate chain. With --tls-key, serves wss/https.
    #[arg(long, env = "CREW_ROUTER_TLS_CERT", requires = "tls_key")]
    tls_cert: Option<PathBuf>,

    /// PEM private key.
    #[arg(long, env = "CREW_ROUTER_TLS_KEY", requires = "tls_cert")]
    tls_key: Option<PathBuf>,
}

fn days(n: u32) -> Duration {
    Duration::from_secs(u64::from(n) * 86_400)
}

fn default_title() -> String {
    gethostname::gethostname()
        .into_string()
        .ok()
        .map(|h| h.split('.').next().unwrap_or_default().to_string())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "crew-router".into())
}

async fn serve(data: PathBuf, args: ServeArgs) -> Result<()> {
    let signup = match args.signup {
        Signup::Open => SignupMode::Open,
        Signup::Invite => SignupMode::Invite,
        Signup::Closed => SignupMode::Closed,
    };
    let invite_code = args.invite_code.filter(|c| !c.is_empty());
    if signup == SignupMode::Invite && invite_code.is_none() {
        bail!("--signup invite needs --invite-code (or CREW_ROUTER_INVITE_CODE)");
    }
    if args.ttl_days == 0 {
        bail!("--ttl-days must be at least 1");
    }
    let hub = HubConfig {
        title: args.title.unwrap_or_else(default_title),
        signup,
        invite_code,
        ttl: days(args.ttl_days),
    };
    let mut cfg = ServeConfig::new(data, hub);
    cfg.tls = args.tls_cert.zip(args.tls_key);
    tracing::info!(
        version = crew_router::hub::VERSION,
        title = %cfg.hub.title,
        signup = signup.as_str(),
        ttl_days = args.ttl_days,
        data = %cfg.data_dir.display(),
        "starting crew-router"
    );
    let listener = std::net::TcpListener::bind(args.listen)
        .with_context(|| format!("cannot listen on {}", args.listen))?;
    server::run(listener, cfg, shutdown_signal()).await
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = term => {},
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            EnvFilter::try_from_env("CREW_ROUTER_LOG").unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    match cli.command {
        Command::Serve(args) => tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(serve(cli.data, args)),
        Command::Users { command } => {
            let out = match command {
                UsersCommand::List => admin::users_list(&cli.data)?,
                UsersCommand::Kick { name, ttl_days } => {
                    admin::users_kick(&cli.data, &name, days(ttl_days))?
                }
                UsersCommand::Rename { old, new } => admin::users_rename(&cli.data, &old, &new)?,
            };
            println!("{out}");
            Ok(())
        }
        Command::Stats => {
            println!("{}", admin::stats(&cli.data)?);
            Ok(())
        }
    }
}
