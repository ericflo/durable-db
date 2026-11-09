pub mod api;
pub mod auth;
pub mod backup;
pub mod cache;
mod directory;
pub mod engine;
mod measured;
pub mod metrics;
mod observation;
pub mod providers;
pub mod publication;
pub mod storage;
pub mod upload_budget;

use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Invalid(String),
    #[error("revision conflict; read a fresh snapshot before retrying")]
    Conflict,
    #[error("database not found")]
    NotFound,
    #[error("authentication or permission denied")]
    Unauthorized,
    #[error("{0}")]
    Limit(String),
    #[error("server is at capacity; retry with backoff")]
    Busy,
    #[error("{0}")]
    Storage(#[from] anyhow::Error),
}
pub type Result<T> = std::result::Result<T, Error>;

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        #[derive(Serialize)]
        struct Body {
            error: &'static str,
            message: String,
        }
        let (status, code) = match &self {
            Self::Invalid(_) => (StatusCode::BAD_REQUEST, "invalid_request"),
            Self::Conflict => (StatusCode::CONFLICT, "conflict"),
            Self::NotFound => (StatusCode::NOT_FOUND, "not_found"),
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized"),
            Self::Limit(_) => (StatusCode::PAYLOAD_TOO_LARGE, "limit_exceeded"),
            Self::Busy => (StatusCode::TOO_MANY_REQUESTS, "busy"),
            Self::Storage(_) => (StatusCode::SERVICE_UNAVAILABLE, "storage_unavailable"),
        };
        let message = if matches!(self, Self::Storage(_)) {
            // Upstream errors may include URLs, object names, or credentials. Do not expose them.
            "storage operation failed; a write outcome may be unknown; retry the identical request"
                .into()
        } else {
            self.to_string()
        };
        (
            status,
            Json(Body {
                error: code,
                message,
            }),
        )
            .into_response()
    }
}
