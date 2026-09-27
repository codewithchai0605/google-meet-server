use axum::extract::ws::WebSocketUpgrade;
use axum::extract::{Path, Query, State};
use axum::response::Response;
use serde::Deserialize;

use crate::rtc::signaling::{self, WsConnectParams};
use crate::state::AppState;

#[derive(Deserialize)]
pub struct WsQuery {
    pub token: String,
}

pub async fn upgrade(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
    Path(code): Path<String>,
    Query(query): Query<WsQuery>,
) -> Response {
    ws.on_upgrade(move |socket| async move {
        signaling::handle_socket(
            socket,
            state,
            WsConnectParams {
                meeting_code: code,
                token: query.token,
            },
        )
        .await;
    })
}
