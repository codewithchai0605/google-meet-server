use anyhow::{Context, Result, anyhow};
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;

/// Process-wide configuration, parsed once from the environment at startup.
///
/// Uses `Arc<str>` for string fields so that cloning `Config` across threads/handlers
/// is cheap (pointer copy + atomic increment, zero allocation).
#[derive(Debug, Clone)]
pub struct Config {
    pub http_addr: SocketAddr,
    pub database_url: Arc<str>,
    pub jwt_secret: Arc<str>,
    pub jwt_expiry_hours: i64,

    /// Local IP mediasoup workers bind their RTP/RTCP sockets to (usually 0.0.0.0).
    pub mediasoup_listen_ip: IpAddr,
    /// Public IP/hostname advertised in ICE candidates for NAT traversal.
    pub mediasoup_announced_ip: Option<Arc<str>>,
    pub mediasoup_min_port: u16,
    pub mediasoup_max_port: u16,
    /// Number of mediasoup worker processes bound to CPU cores.
    pub mediasoup_num_workers: usize,

    pub recordings_dir: PathBuf,
    pub cors_origins: Arc<[Arc<str>]>,

    /// Optional TURN relay details.
    pub turn_url: Option<Arc<str>>,
    pub turn_username: Option<Arc<str>>,
    pub turn_credential: Option<Arc<str>>,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let _ = dotenvy::dotenv();

        // 1. Parsed directly to SocketAddr so server startup doesn't fail later on bad format
        let http_addr: SocketAddr = parse_env_or("HTTP_ADDR", "0.0.0.0:8080")?;
        let database_url = env_required("DATABASE_URL")?.into();
        let jwt_secret = env_required("JWT_SECRET")?.into();
        let jwt_expiry_hours = parse_env_or("JWT_EXPIRY_HOURS", "168")?;

        let mediasoup_listen_ip: IpAddr = parse_env_or("MEDIASOUP_LISTEN_IP", "0.0.0.0")?;
        let mediasoup_announced_ip = env_optional("MEDIASOUP_ANNOUNCED_IP");
        let mediasoup_min_port = parse_env_or("MEDIASOUP_MIN_PORT", "40000")?;
        let mediasoup_max_port = parse_env_or("MEDIASOUP_MAX_PORT", "49999")?;

        let user_workers: usize = parse_env_or("MEDIASOUP_NUM_WORKERS", "0")?;
        let mediasoup_num_workers = if user_workers == 0 {
            thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        } else {
            user_workers
        };

        let recordings_dir = PathBuf::from(
            std::env::var("RECORDINGS_DIR").unwrap_or_else(|_| "./recordings".into()),
        );

        let cors_raw = std::env::var("CORS_ORIGINS").unwrap_or_else(|_| "*".into());
        let cors_origins: Arc<[Arc<str>]> = cors_raw
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(Arc::from)
            .collect();

        Ok(Self {
            http_addr,
            database_url,
            jwt_secret,
            jwt_expiry_hours,
            mediasoup_listen_ip,
            mediasoup_announced_ip,
            mediasoup_min_port,
            mediasoup_max_port,
            mediasoup_num_workers,
            recordings_dir,
            cors_origins,
            turn_url: env_optional("TURN_URL"),
            turn_username: env_optional("TURN_USERNAME"),
            turn_credential: env_optional("TURN_CREDENTIAL"),
        })
    }
}

// Helpers avoiding allocations

fn env_required(key: &str) -> Result<String> {
    std::env::var(key).map_err(|_| anyhow!("missing required environment variable: {key}"))
}

fn env_optional(key: &str) -> Option<Arc<str>> {
    std::env::var(key).ok().map(Arc::from)
}

/// Parses an environment variable or default string directly into target type T.
/// Fails early with a context message if the value is malformed.
fn parse_env_or<T>(key: &str, default: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match std::env::var(key) {
        Ok(val) => val
            .parse::<T>()
            .with_context(|| format!("failed to parse environment variable {key}={val:?}")),
        Err(_) => default
            .parse::<T>()
            .with_context(|| format!("failed to parse fallback default for {key}")),
    }
}
