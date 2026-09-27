use axum::extract::State;
use axum::Json;
use serde_json::{json, Value};

use crate::state::AppState;

/// Public (no auth required — it's needed before login to prime the WebRTC stack, and it
/// contains nothing sensitive that a TURN credential rotation policy wouldn't already assume is
/// visible to any client).
pub async fn ice_servers(State(state): State<AppState>) -> Json<Value> {
    let mut servers = vec![json!({ "urls": ["stun:stun.l.google.com:19302"] })];

    if let (Some(url), Some(username), Some(credential)) = (
        &state.config.turn_url,
        &state.config.turn_username,
        &state.config.turn_credential,
    ) {
        servers.push(json!({
            "urls": [url],
            "username": username,
            "credential": credential,
        }));
    }

    Json(json!({ "iceServers": servers }))
}
