use axum::extract::State;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::auth::password::{hash_password, verify_password};
use crate::auth::{jwt, AuthUser};
use crate::db;
use crate::db::models::PublicUser;
use crate::error::{AppError, AppResult};
use crate::state::AppState;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterRequest {
    pub email: String,
    pub password: String,
    #[serde(alias = "display_name")]
    pub display_name: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Serialize)]
pub struct AuthResponse {
    pub token: String,
    pub user: PublicUser,
}

pub async fn register(State(state): State<AppState>, Json(req): Json<RegisterRequest>) -> AppResult<Json<AuthResponse>> {
    let email = req.email.trim().to_lowercase();
    if !email.contains('@') || email.len() > 254 {
        return Err(AppError::BadRequest("invalid email".into()));
    }
    if req.password.len() < 8 {
        return Err(AppError::BadRequest("password must be at least 8 characters".into()));
    }
    let display_name = req.display_name.trim();
    if display_name.is_empty() || display_name.len() > 100 {
        return Err(AppError::BadRequest("invalid display name".into()));
    }

    if db::users::find_by_email(&state.db, &email).await?.is_some() {
        return Err(AppError::Conflict("an account with that email already exists".into()));
    }

    let password_hash = hash_password(&req.password)?;
    let user = db::users::create(&state.db, &email, &password_hash, display_name).await?;

    let token = jwt::issue_token(
        &state.config.jwt_secret,
        user.id,
        &user.email,
        &user.display_name,
        state.config.jwt_expiry_hours,
    )?;

    Ok(Json(AuthResponse {
        token,
        user: user.into(),
    }))
}

pub async fn login(State(state): State<AppState>, Json(req): Json<LoginRequest>) -> AppResult<Json<AuthResponse>> {
    let email = req.email.trim().to_lowercase();
    let user = db::users::find_by_email(&state.db, &email)
        .await?
        .ok_or(AppError::Unauthorized)?;

    if !verify_password(&req.password, &user.password_hash) {
        return Err(AppError::Unauthorized);
    }

    let token = jwt::issue_token(
        &state.config.jwt_secret,
        user.id,
        &user.email,
        &user.display_name,
        state.config.jwt_expiry_hours,
    )?;

    Ok(Json(AuthResponse {
        token,
        user: user.into(),
    }))
}

pub async fn me(AuthUser(claims): AuthUser) -> Json<Value> {
    Json(json!({
        "id": claims.sub,
        "email": claims.email,
        "displayName": claims.display_name,
    }))
}
