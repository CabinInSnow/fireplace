use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

/// Trait that every fireplace-managed entity must implement.
///
/// # Implementing
/// Implement this on your plain Rust struct in the game-server crate.
/// You do **not** need to import sea-orm; the engine handles all ORM details
/// internally using the metadata this trait exposes.
///
/// ```rust
/// #[derive(Clone, Serialize, Deserialize)]
/// pub struct Hero {
///     pub id: i64,
///     pub uid: i64,
///     pub hero_type: String,
///     pub level: i32,
///     pub exp: i32,
/// }
///
/// impl FireplaceEntity for Hero {
///     fn table_name() -> &'static str { "heroes" }
///     fn primary_key() -> &'static str { "id" }
///     fn pk_value(&self) -> Value { serde_json::json!(self.id) }
/// }
/// ```
pub trait FireplaceEntity: Serialize + DeserializeOwned + Clone + Send + Sync + Unpin + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> + 'static {
    /// The database table name (e.g. `"heroes"`).
    fn table_name() -> &'static str;

    /// The primary key column name (e.g. `"id"`).
    fn primary_key() -> &'static str;

    /// The value of this instance's primary key, serialised as JSON.
    fn pk_value(&self) -> Value;
}
