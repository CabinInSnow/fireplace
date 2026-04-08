use axum::{
    extract::State,
    response::{IntoResponse, Json},
};
use cabin_fireplace::core::server::FirePlaceState;
use serde_json::json;

use crate::state::AppState;

pub async fn hello_world(State(state): State<FirePlaceState<AppState>>) -> impl IntoResponse {
    Json(json!({
        "message": "Hello from cabin-fireplace HTTP Endpoint!",
        "dummy_db_flag": state.user_state.dummy_db_flag
    }))
}
