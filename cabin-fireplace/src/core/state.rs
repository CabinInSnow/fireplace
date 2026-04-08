use redis::aio::MultiplexedConnection;
use sqlx::PgPool;

use crate::db::manager::DbManager;

#[derive(Clone)]
pub struct StateBundle {
    pub db_manager: DbManager,
    /// Raw pool — used internally by the engine only.
    pub(crate) pool: PgPool,
    pub redis_data: MultiplexedConnection,
    pub redis_pubsub: MultiplexedConnection,
}

impl StateBundle {
    pub fn new(
        pool: PgPool,
        redis_data: MultiplexedConnection,
        redis_pubsub: MultiplexedConnection,
    ) -> Self {
        Self {
            db_manager: DbManager::new(pool.clone()),
            pool,
            redis_data,
            redis_pubsub,
        }
    }
}
