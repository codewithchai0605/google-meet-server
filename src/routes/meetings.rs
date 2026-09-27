use axum::Json;
use axum::extract::{Path, State};
use nanoid::nanoid;
use serde::{Deserialize, Serialize};

use crate::auth::AuthUser;
use crate::db;
use crate::db::models::Meeting;
use crate::error::{AppError, AppResult};
use crate::rtc::protocol::ServerEvent;
use crate::state::AppState;

/// Excludes visually ambiguous characters (0/O, 1/I/l) since join codes get read aloud and typed
/// by hand.
const CODE_ALPHABET: [char; 31] = [
    '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm',
    'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

fn generate_code() -> String {
    let raw = nanoid!(10, &CODE_ALPHABET);
    format!("{}-{}-{}", &raw[0..3], &raw[3..7], &raw[7..10])
}

#[derive(Deserialize)]
pub struct CreateMeetingRequest {
    pub title: Option<String>,
    #[serde(default = "default_max_participants")]
    pub max_participants: i16,
    #[serde(default = "default_true")]
    pub waiting_room_enabled: bool,
}

fn default_max_participants() -> i16 {
    50
}
fn default_true() -> bool {
    true
}

#[derive(Serialize)]
pub struct MeetingResponse {
    #[serde(flatten)]
    pub meeting: Meeting,
    pub join_url_path: String,
}

impl From<Meeting> for MeetingResponse {
    fn from(meeting: Meeting) -> Self {
        let join_url_path = format!("/join/{}", meeting.code);
        Self {
            meeting,
            join_url_path,
        }
    }
}

pub async fn create(
    State(state): State<AppState>,
    AuthUser(claims): AuthUser,
    Json(req): Json<CreateMeetingRequest>,
) -> AppResult<Json<MeetingResponse>> {
    if !(2..=500).contains(&req.max_participants) {
        return Err(AppError::BadRequest(
            "maxParticipants must be between 2 and 500".into(),
        ));
    }
    let title = req
        .title
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
        .unwrap_or_else(|| format!("{}'s meeting", claims.display_name));

    // Collisions are astronomically unlikely (31^10 space) but a unique index backs this up
    // regardless; retry once on the rare conflict rather than trusting probability alone.
    for _ in 0..3 {
        let code = generate_code();
        match db::meetings::create(
            &state.db,
            &code,
            &title,
            claims.sub,
            req.max_participants,
            req.waiting_room_enabled,
        )
        .await
        {
            Ok(meeting) => return Ok(Json(meeting.into())),
            Err(sqlx::Error::Database(e)) if e.is_unique_violation() => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Err(AppError::Internal(anyhow::anyhow!(
        "failed to allocate a unique meeting code"
    )))
}

pub async fn list_mine(
    State(state): State<AppState>,
    AuthUser(claims): AuthUser,
) -> AppResult<Json<Vec<MeetingResponse>>> {
    let meetings = db::meetings::list_for_user(&state.db, claims.sub).await?;
    Ok(Json(meetings.into_iter().map(Into::into).collect()))
}

pub async fn get_by_code(
    State(state): State<AppState>,
    AuthUser(_claims): AuthUser,
    Path(code): Path<String>,
) -> AppResult<Json<MeetingResponse>> {
    let meeting = db::meetings::find_by_code(&state.db, &code)
        .await?
        .ok_or_else(|| AppError::NotFound("meeting not found".into()))?;

    let occupancy = state.rooms.get(&code).map(|r| r.occupancy()).unwrap_or(0);
    if occupancy as i16 >= meeting.max_participants && meeting.status != "ended" {
        // Still return the meeting (client shows "meeting is full"), just flag it explicitly
        // rather than a bare 200 that looks joinable.
        return Err(AppError::Conflict("meeting is full".into()));
    }

    Ok(Json(meeting.into()))
}

pub async fn end_meeting(
    State(state): State<AppState>,
    AuthUser(claims): AuthUser,
    Path(code): Path<String>,
) -> AppResult<Json<serde_json::Value>> {
    let meeting = db::meetings::find_by_code(&state.db, &code)
        .await?
        .ok_or_else(|| AppError::NotFound("meeting not found".into()))?;

    if meeting.host_id != claims.sub {
        return Err(AppError::Forbidden(
            "only the host can end this meeting".into(),
        ));
    }

    if let Some(room) = state.rooms.get(&code) {
        room.broadcast_event(ServerEvent::MeetingEnded, serde_json::json!({}), None);
    }
    db::meetings::set_status_ended(&state.db, meeting.id).await?;

    Ok(Json(serde_json::json!({ "ok": true })))
}
