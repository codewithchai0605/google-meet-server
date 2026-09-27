use chrono::Utc;
use sqlx::PgPool;
use uuid::Uuid;

use super::models::Meeting;

pub async fn create(
    pool: &PgPool,
    code: &str,
    title: &str,
    host_id: Uuid,
    max_participants: i16,
    waiting_room_enabled: bool,
) -> Result<Meeting, sqlx::Error> {
    sqlx::query_as::<_, Meeting>(
        r#"
        INSERT INTO meetings (code, title, host_id, max_participants, waiting_room_enabled)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, code, title, host_id, max_participants, waiting_room_enabled,
                  status, created_at, started_at, ended_at
        "#,
    )
    .bind(code)
    .bind(title)
    .bind(host_id)
    .bind(max_participants)
    .bind(waiting_room_enabled)
    .fetch_one(pool)
    .await
}

pub async fn find_by_code(pool: &PgPool, code: &str) -> Result<Option<Meeting>, sqlx::Error> {
    sqlx::query_as::<_, Meeting>(
        r#"
        SELECT id, code, title, host_id, max_participants, waiting_room_enabled,
               status, created_at, started_at, ended_at
        FROM meetings WHERE code = $1
        "#,
    )
    .bind(code)
    .fetch_optional(pool)
    .await
}

pub async fn find_by_id(pool: &PgPool, id: Uuid) -> Result<Option<Meeting>, sqlx::Error> {
    sqlx::query_as::<_, Meeting>(
        r#"
        SELECT id, code, title, host_id, max_participants, waiting_room_enabled,
               status, created_at, started_at, ended_at
        FROM meetings WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await
}

/// Meetings a user hosted or attended, most recent first. Backs the "meeting history" screen.
pub async fn list_for_user(pool: &PgPool, user_id: Uuid) -> Result<Vec<Meeting>, sqlx::Error> {
    sqlx::query_as::<_, Meeting>(
        r#"
        SELECT DISTINCT m.id, m.code, m.title, m.host_id, m.max_participants,
               m.waiting_room_enabled, m.status, m.created_at, m.started_at, m.ended_at
        FROM meetings m
        LEFT JOIN meeting_participants mp ON mp.meeting_id = m.id
        WHERE m.host_id = $1 OR mp.user_id = $1
        ORDER BY m.created_at DESC
        LIMIT 100
        "#,
    )
    .bind(user_id)
    .fetch_all(pool)
    .await
}

pub async fn set_status_live(pool: &PgPool, id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"UPDATE meetings SET status = 'live', started_at = COALESCE(started_at, $2) WHERE id = $1"#,
    )
    .bind(id)
    .bind(Utc::now())
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn set_status_ended(pool: &PgPool, id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(r#"UPDATE meetings SET status = 'ended', ended_at = $2 WHERE id = $1"#)
        .bind(id)
        .bind(Utc::now())
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn log_participant_join(
    pool: &PgPool,
    meeting_id: Uuid,
    user_id: Uuid,
    role: &str,
) -> Result<Uuid, sqlx::Error> {
    let (id,): (Uuid,) = sqlx::query_as(
        r#"
        INSERT INTO meeting_participants (meeting_id, user_id, role)
        VALUES ($1, $2, $3)
        RETURNING id
        "#,
    )
    .bind(meeting_id)
    .bind(user_id)
    .bind(role)
    .fetch_one(pool)
    .await?;
    Ok(id)
}

pub async fn log_participant_leave(pool: &PgPool, participant_row_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(r#"UPDATE meeting_participants SET left_at = $2 WHERE id = $1"#)
        .bind(participant_row_id)
        .bind(Utc::now())
        .execute(pool)
        .await?;
    Ok(())
}
