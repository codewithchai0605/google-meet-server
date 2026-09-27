use chrono::Utc;
use sqlx::PgPool;
use uuid::Uuid;

use super::models::Recording;

pub async fn create(
    pool: &PgPool,
    meeting_id: Uuid,
    participant_user_id: Option<Uuid>,
    kind: &str,
    file_path: &str,
) -> Result<Recording, sqlx::Error> {
    sqlx::query_as::<_, Recording>(
        r#"
        INSERT INTO recordings (meeting_id, participant_user_id, kind, file_path)
        VALUES ($1, $2, $3, $4)
        RETURNING id, meeting_id, participant_user_id, kind, file_path, status, started_at, ended_at
        "#,
    )
    .bind(meeting_id)
    .bind(participant_user_id)
    .bind(kind)
    .bind(file_path)
    .fetch_one(pool)
    .await
}

pub async fn mark_status(pool: &PgPool, id: Uuid, status: &str) -> Result<(), sqlx::Error> {
    let ended_at = matches!(status, "ready" | "failed").then(Utc::now);
    sqlx::query(r#"UPDATE recordings SET status = $2, ended_at = COALESCE($3, ended_at) WHERE id = $1"#)
        .bind(id)
        .bind(status)
        .bind(ended_at)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn list_for_meeting(pool: &PgPool, meeting_id: Uuid) -> Result<Vec<Recording>, sqlx::Error> {
    sqlx::query_as::<_, Recording>(
        r#"
        SELECT id, meeting_id, participant_user_id, kind, file_path, status, started_at, ended_at
        FROM recordings WHERE meeting_id = $1 ORDER BY started_at DESC
        "#,
    )
    .bind(meeting_id)
    .fetch_all(pool)
    .await
}

pub async fn find_by_id(pool: &PgPool, id: Uuid) -> Result<Option<Recording>, sqlx::Error> {
    sqlx::query_as::<_, Recording>(
        r#"
        SELECT id, meeting_id, participant_user_id, kind, file_path, status, started_at, ended_at
        FROM recordings WHERE id = $1
        "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await
}
