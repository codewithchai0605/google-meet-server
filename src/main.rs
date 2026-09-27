mod auth;
mod config;
mod db;
mod error;
mod routes;
mod rtc;
mod state;

use std::sync::Arc;

use config::Config;
use state::{AppState, RoomRegistry};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,mediasoup=warn")),
        )
        .init();

    let config = Arc::new(Config::from_env()?);
    tracing::info!(workers = config.mediasoup_num_workers, "starting mediasoup-meet server");

    std::fs::create_dir_all(&config.recordings_dir)?;

    let db = db::connect(&config.database_url).await?;
    tracing::info!("database connected and migrations applied");

    let rooms = RoomRegistry::new(&config).await?;

    let state = AppState {
        config: config.clone(),
        db,
        rooms,
    };

    let app = routes::build(state);

    let listener = tokio::net::TcpListener::bind(&config.http_addr).await?;
    tracing::info!(addr = %config.http_addr, "listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received");
}
