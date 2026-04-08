/// DB schema initialization utility.
/// Reads docs/schema.sql and executes it against the configured Postgres instance.
use sqlx::postgres::PgPoolOptions;
use std::fs;

#[tokio::main]
async fn main() {
    let db_url = "postgres://user:pass@localhost/db";
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(db_url)
        .await
        .expect("Failed to connect to database");

    let schema_sql = fs::read_to_string("docs/schema.sql")
        .expect("Failed to read schema.sql");

    sqlx::raw_sql(&schema_sql)
        .execute(&pool)
        .await
        .expect("Failed to execute schema.sql");

    println!("Schema applied successfully!");
}
