use sqlx::{postgres::PgArguments, Arguments, PgPool, Postgres, Transaction};
use serde_json::Value;
use std::marker::PhantomData;

use super::entity::FireplaceEntity;
use super::query::build_select_statement;
use crate::sync::{GameContext, SyncAction, SyncEvent};
use crate::error::{FireplaceError, Result};

// ── DbManager ────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct DbManager {
    pub(crate) pool: PgPool,
}

impl DbManager {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn find_by_id<T: FireplaceEntity>(
        &self,
        id: impl Into<Value>,
    ) -> Result<Option<T>> {
        let sql = format!(
            "SELECT * FROM \"{}\" WHERE \"{}\" = $1 LIMIT 1",
            T::table_name(),
            T::primary_key()
        );
        execute_find_one(&self.pool, &sql, &[id.into()]).await
    }

    pub fn find_by<T: FireplaceEntity>(
        &self,
        column: &str,
        value: impl Into<Value>,
    ) -> DbFindQuery<T> {
        DbFindQuery {
            pool: self.pool.clone(),
            filters: vec![(column.to_string(), value.into())],
            table: T::table_name(),
            _phantom: PhantomData,
        }
    }

    pub async fn begin(&self) -> Result<TxnManager> {
        let txn = self.pool.begin().await?;
        Ok(TxnManager::new(txn))
    }

    pub async fn insert<T: FireplaceEntity>(&self, entity: &T) -> Result<T> {
        let val = serde_json::to_value(entity)?;
        raw_insert(&self.pool, T::table_name(), val).await
    }
}

// ── DbFindQuery ──────────────────────────────────────────────────────────────

pub struct DbFindQuery<T: FireplaceEntity> {
    pool: PgPool,
    table: &'static str,
    filters: Vec<(String, Value)>,
    _phantom: PhantomData<T>,
}

impl<T: FireplaceEntity> DbFindQuery<T> {
    pub fn and(mut self, column: &str, value: impl Into<Value>) -> Self {
        self.filters.push((column.to_string(), value.into()));
        self
    }

    pub async fn one(self) -> Result<Option<T>> {
        let (base, vals) = build_select_statement(self.table, &self.filters);
        let sql = format!("{} LIMIT 1", base);
        execute_find_one(&self.pool, &sql, &vals).await
    }

    pub async fn all(self) -> Result<Vec<T>> {
        let (base, vals) = build_select_statement(self.table, &self.filters);
        execute_find_all(&self.pool, &base, &vals).await
    }
}

// ── TxnManager ───────────────────────────────────────────────────────────────

pub struct TxnManager {
    pub(crate) txn: Transaction<'static, Postgres>,
    pub(crate) sync_events: Vec<SyncEvent>,
    pub(crate) outbound: Vec<(u64, Vec<SyncEvent>)>,
}

impl TxnManager {
    pub(crate) fn new(txn: Transaction<'static, Postgres>) -> Self {
        Self { txn, sync_events: Vec::new(), outbound: Vec::new() }
    }

    // ── Reads ─────────────────────────────────────────────────────────────────

    pub async fn find_by_id<T: FireplaceEntity>(
        &mut self,
        id: impl Into<Value>,
    ) -> Result<Option<T>> {
        let sql = format!(
            "SELECT * FROM \"{}\" WHERE \"{}\" = $1 LIMIT 1",
            T::table_name(),
            T::primary_key()
        );
        execute_find_one(&mut *self.txn, &sql, &[id.into()]).await
    }

    pub fn find_by<T: FireplaceEntity>(
        &mut self,
        column: &str,
        value: impl Into<Value>,
    ) -> TxnFindQuery<'_, T> {
        TxnFindQuery {
            txn: &mut self.txn,
            filters: vec![(column.to_string(), value.into())],
            table: T::table_name(),
            _phantom: PhantomData,
        }
    }

    // ── Writes ────────────────────────────────────────────────────────────────

    pub async fn insert<T: FireplaceEntity>(&mut self, entity: T) -> Result<T> {
        let val = serde_json::to_value(&entity)?;
        let saved: T = raw_insert(&mut *self.txn, T::table_name(), val).await?;
        self.sync_events.push(SyncEvent {
            domain: T::table_name().to_string(),
            id: saved.pk_value(),
            action: SyncAction::Upsert,
            payload: serde_json::to_value(&saved).unwrap_or_default(),
        });
        Ok(saved)
    }

    pub async fn update<T: FireplaceEntity>(&mut self, entity: T) -> Result<T> {
        let val = serde_json::to_value(&entity)?;
        let saved: T = raw_update(&mut *self.txn, T::table_name(), T::primary_key(), val).await?;
        self.sync_events.push(SyncEvent {
            domain: T::table_name().to_string(),
            id: saved.pk_value(),
            action: SyncAction::Upsert,
            payload: serde_json::to_value(&saved).unwrap_or_default(),
        });
        Ok(saved)
    }

    pub async fn delete<T: FireplaceEntity>(&mut self, id: impl Into<Value>) -> Result<()> {
        let id_val = id.into();
        let sql = format!(
            "DELETE FROM \"{}\" WHERE \"{}\" = $1",
            T::table_name(),
            T::primary_key()
        );
        let args = build_args(&[id_val.clone()])?;
        sqlx::query_with(&sql, args)
            .execute(&mut *self.txn)
            .await?;
        self.sync_events.push(SyncEvent {
            domain: T::table_name().to_string(),
            id: id_val,
            action: SyncAction::Delete,
            payload: serde_json::json!({}),
        });
        Ok(())
    }

    pub fn publish_to(&mut self, target_uid: u64, events: Vec<SyncEvent>) {
        if !events.is_empty() {
            self.outbound.push((target_uid, events));
        }
    }

    pub fn into_ctx(self) -> GameContext {
        GameContext::from_txn_manager(self)
    }

    pub async fn commit(self) -> Result<()> {
        self.txn.commit().await?;
        Ok(())
    }
}

// ── TxnFindQuery ─────────────────────────────────────────────────────────────

pub struct TxnFindQuery<'a, T: FireplaceEntity> {
    txn: &'a mut Transaction<'static, Postgres>,
    table: &'static str,
    filters: Vec<(String, Value)>,
    _phantom: PhantomData<T>,
}

impl<'a, T: FireplaceEntity> TxnFindQuery<'a, T> {
    pub fn and(mut self, column: &str, value: impl Into<Value>) -> Self {
        self.filters.push((column.to_string(), value.into()));
        self
    }

    pub async fn one(self) -> Result<Option<T>> {
        let (base, vals) = build_select_statement(self.table, &self.filters);
        let sql = format!("{} LIMIT 1", base);
        execute_find_one(&mut **self.txn, &sql, &vals).await
    }

    pub async fn all(self) -> Result<Vec<T>> {
        let (base, vals) = build_select_statement(self.table, &self.filters);
        execute_find_all(&mut **self.txn, &base, &vals).await
    }
}

// ── Parameter binding ─────────────────────────────────────────────────────────

fn build_args(vals: &[Value]) -> Result<PgArguments> {
    let mut args = PgArguments::default();
    for val in vals {
        match val {
            Value::Null      => args.add(None::<i64>)?,
            Value::Bool(b)   => args.add(*b)?,
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    args.add(i)?;
                } else if let Some(f) = n.as_f64() {
                    args.add(f)?;
                }
            }
            Value::String(s) => args.add(s.clone())?,
            other            => args.add(other.to_string())?,
        }
    }
    Ok(args)
}

// ── Low-level query helpers ───────────────────────────────────────────────────

async fn execute_find_one<'e, E, T>(executor: E, sql: &str, vals: &[Value]) -> Result<Option<T>>
where
    E: sqlx::Executor<'e, Database = Postgres>,
    T: Unpin + Send + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>,
{
    let args = build_args(vals)?;
    let row = sqlx::query_as_with::<_, T, _>(sql, args)
        .fetch_optional(executor)
        .await?;
    Ok(row)
}

async fn execute_find_all<'e, E, T>(executor: E, sql: &str, vals: &[Value]) -> Result<Vec<T>>
where
    E: sqlx::Executor<'e, Database = Postgres>,
    T: Unpin + Send + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>,
{
    let args = build_args(vals)?;
    let rows = sqlx::query_as_with::<_, T, _>(sql, args)
        .fetch_all(executor)
        .await?;
    Ok(rows)
}

/// INSERT ... RETURNING * — null 필드 스킵 (DB default 적용).
async fn raw_insert<'e, E, T>(executor: E, table: &str, val: Value) -> Result<T>
where
    E: sqlx::Executor<'e, Database = Postgres>,
    T: Unpin + Send + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>,
{
    let obj = val.as_object().ok_or_else(|| FireplaceError::Internal("Entity must serialise to a JSON object".to_string()))?;
    let non_null: Vec<(&str, &Value)> = obj.iter()
        .filter(|(_, v)| !v.is_null())
        .map(|(k, v)| (k.as_str(), v))
        .collect();
    if non_null.is_empty() {
        return Err(FireplaceError::Internal("Entity has no non-null fields to insert".to_string()));
    }
    let col_list   = non_null.iter().map(|(c, _)| format!("\"{}\"", c)).collect::<Vec<_>>().join(", ");
    let placeholders = (1..=non_null.len()).map(|i| format!("${}", i)).collect::<Vec<_>>().join(", ");
    let sql = format!(
        "INSERT INTO \"{}\" ({}) VALUES ({}) RETURNING *",
        table, col_list, placeholders
    );
    let bind_vals: Vec<Value> = non_null.iter().map(|(_, v)| (*v).clone()).collect();
    let args = build_args(&bind_vals)?;
    sqlx::query_as_with::<_, T, _>(&sql, args)
        .fetch_optional(executor)
        .await?
        .ok_or_else(|| FireplaceError::Internal("INSERT RETURNING returned no row".to_string()))
}

/// UPDATE ... RETURNING * — pk를 $1로, 나머지 컬럼을 $2..N으로 바인딩.
async fn raw_update<'e, E, T>(executor: E, table: &str, pk_col: &str, val: Value) -> Result<T>
where
    E: sqlx::Executor<'e, Database = Postgres>,
    T: Unpin + Send + for<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow>,
{
    let obj = val.as_object().ok_or_else(|| FireplaceError::Internal("Entity must serialise to a JSON object".to_string()))?;
    let pk_val = obj.get(pk_col)
        .ok_or_else(|| FireplaceError::Internal(format!("Primary key '{}' not found in entity", pk_col)))?
        .clone();
    let set_cols: Vec<(&str, &Value)> = obj.iter()
        .filter(|(k, _)| k.as_str() != pk_col)
        .map(|(k, v)| (k.as_str(), v))
        .collect();
    if set_cols.is_empty() {
        return Err(FireplaceError::Internal("No columns to update".to_string()));
    }
    let set_clause = set_cols.iter().enumerate()
        .map(|(i, (col, _))| format!("\"{}\" = ${}", col, i + 2))
        .collect::<Vec<_>>().join(", ");
    let sql = format!(
        "UPDATE \"{}\" SET {} WHERE \"{}\" = $1 RETURNING *",
        table, set_clause, pk_col
    );
    let mut bind_vals = vec![pk_val];
    bind_vals.extend(set_cols.iter().map(|(_, v)| (*v).clone()));
    let args = build_args(&bind_vals)?;
    sqlx::query_as_with::<_, T, _>(&sql, args)
        .fetch_optional(executor)
        .await?
        .ok_or_else(|| FireplaceError::Internal("UPDATE RETURNING returned no row".to_string()))
}
