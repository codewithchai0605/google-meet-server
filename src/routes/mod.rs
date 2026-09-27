pub mod auth;
pub mod config;
pub mod health;
pub mod meetings;
pub mod recordings;
pub mod ws;

use std::sync::Arc;

use axum::Router;
use axum::http::HeaderValue;
use axum::routing::{get, post};
use tower_http::compression::CompressionLayer;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::state::AppState;

pub fn build(state: AppState) -> Router {
    let cors = build_cors(&state.config.cors_origins);

    let compression_layer = CompressionLayer::new()
        .gzip(true)
        .br(true)
        .zstd(true);

    Router::new()
        .route("/health", get(health::health))
        .route("/api/config/ice-servers", get(config::ice_servers))
        .route("/api/auth/register", post(auth::register))
        .route("/api/auth/login", post(auth::login))
        .route("/api/me", get(auth::me))
        .route(
            "/api/meetings",
            post(meetings::create).get(meetings::list_mine),
        )
        .route("/api/meetings/{code}", get(meetings::get_by_code))
        .route("/api/meetings/{code}/end", post(meetings::end_meeting))
        .route(
            "/api/meetings/{code}/recordings",
            get(recordings::list_for_meeting),
        )
        .route("/api/recordings/{id}/download", get(recordings::download))
        .route("/ws/{code}", get(ws::upgrade))
        .layer(compression_layer)
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
}

fn build_cors(origins: &[Arc<str>]) -> CorsLayer {
    let layer = CorsLayer::new().allow_methods(Any).allow_headers(Any);

    if origins.iter().any(|o| &**o == "*") {
        layer.allow_origin(Any)
    } else {
        let parsed: Vec<HeaderValue> = origins.iter().filter_map(|o| o.parse().ok()).collect();
        layer.allow_origin(parsed)
    }
}
