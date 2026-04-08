use thiserror::Error;

#[derive(Error, Debug)]
pub enum FireplaceError {
    #[error("Database error: {0}")]
    Db(#[from] sqlx::Error),

    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Entity not found: {0}")]
    NotFound(String),

    #[error("Authentication failed: {0}")]
    AuthFailed(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Generic error: {0}")]
    Generic(String),
}

pub type Result<T> = std::result::Result<T, FireplaceError>;

impl From<Box<dyn std::error::Error + Send + Sync>> for FireplaceError {
    fn from(e: Box<dyn std::error::Error + Send + Sync>) -> Self {
        FireplaceError::Generic(e.to_string())
    }
}

impl From<String> for FireplaceError {
    fn from(s: String) -> Self {
        FireplaceError::Internal(s)
    }
}

impl From<&str> for FireplaceError {
    fn from(s: &str) -> Self {
        FireplaceError::Internal(s.to_string())
    }
}
