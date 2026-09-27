use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::auth::AuthUser;
use crate::db;
use crate::db::models::Recording;
use crate::error::{AppError, AppResult};
use crate::state::AppState;

async fn require_meeting_host(state: &AppState, meeting_code: &str, user_id: Uuid) -> AppResult<Uuid> {
    let meeting = db::meetings::find_by_code(&state.db, meeting_code)
        .await?
        .ok_or_else(|| AppError::NotFound("meeting not found".into()))?;
    if meeting.host_id != user_id {
        return Err(AppError::Forbidden("only the host can view recordings".into()));
    }
    Ok(meeting.id)
}

pub async fn list_for_meeting(
    State(state): State<AppState>,
    AuthUser(claims): AuthUser,
    Path(code): Path<String>,
) -> AppResult<Json<Vec<Recording>>> {
    let meeting_id = require_meeting_host(&state, &code, claims.sub).await?;
    let recordings = db::recordings::list_for_meeting(&state.db, meeting_id).await?;
    Ok(Json(recordings))
}

pub async fn download(
    State(state): State<AppState>,
    AuthUser(claims): AuthUser,
    Path(recording_id): Path<Uuid>,
) -> AppResult<Response> {
    let recording = db::recordings::find_by_id(&state.db, recording_id)
        .await?
        .ok_or_else(|| AppError::NotFound("recording not found".into()))?;

    let meeting = db::meetings::find_by_id(&state.db, recording.meeting_id)
        .await?
        .ok_or_else(|| AppError::NotFound("meeting not found".into()))?;
    if meeting.host_id != claims.sub {
        return Err(AppError::Forbidden("only the host can download recordings".into()));
    }

    let file = tokio::fs::File::open(&recording.file_path)
        .await
        .map_err(|_| AppError::NotFound("recording file is missing on disk".into()))?;

    let content_type = mime_guess::from_path(&recording.file_path)
        .first_or_octet_stream()
        .to_string();
    let file_name = std::path::Path::new(&recording.file_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("recording")
        .to_string();

    let stream = ReaderStream::new(file);
    let body = Body::from_stream(stream);

    Ok((
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, content_type),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{file_name}\""),
            ),
        ],
        body,
    )
        .into_response())
}
