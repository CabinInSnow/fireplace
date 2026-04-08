use serde_json::Value;

// ── SQL helpers ───────────────────────────────────────────────────────────────

/// Build a `SELECT * FROM {table} WHERE col1=$1 AND col2=$2 ...` statement.
pub(super) fn build_select_statement(
    table: &str,
    filters: &[(String, Value)],
) -> (String, Vec<Value>) {
    if filters.is_empty() {
        return (format!("SELECT * FROM \"{}\"", table), vec![]);
    }

    let clauses: Vec<String> = filters
        .iter()
        .enumerate()
        .map(|(i, (col, _))| format!("\"{}\" = ${}", col, i + 1))
        .collect();

    let sql = format!(
        "SELECT * FROM \"{}\" WHERE {}",
        table,
        clauses.join(" AND ")
    );
    let vals: Vec<Value> = filters.iter().map(|(_, v)| v.clone()).collect();
    (sql, vals)
}

/// Deserialise a `serde_json::Value` row map into `T`.
#[allow(dead_code)]
pub(super) fn from_json_row<T: serde::de::DeserializeOwned>(row: Value) -> Result<T, String> {
    serde_json::from_value(row).map_err(|e| format!("Deserialise error: {}", e))
}
