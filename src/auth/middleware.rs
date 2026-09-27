use axum::extract::{FromRef, FromRequestParts};
use axum::http::request::Parts;
use axum::RequestPartsExt;
use axum_extra::headers::{authorization::Bearer, Authorization};
use axum_extra::TypedHeader;

use crate::auth::jwt::{verify_token, Claims};
use crate::error::AppError;
use crate::state::AppState;

/// Drop-in extractor for any handler that requires a logged-in user:
/// `async fn handler(user: AuthUser, ...)`. Rejects with 401 before the handler body runs if the
/// token is missing, malformed, or expired.
pub struct AuthUser(pub Claims);

impl<S> FromRequestParts<S> for AuthUser
where
    AppState: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let TypedHeader(Authorization(bearer)) = parts
            .extract::<TypedHeader<Authorization<Bearer>>>()
            .await
            .map_err(|_| AppError::Unauthorized)?;

        let app_state = AppState::from_ref(state);
        let claims = verify_token(&app_state.config.jwt_secret, bearer.token())?;
        Ok(AuthUser(claims))
    }
}
