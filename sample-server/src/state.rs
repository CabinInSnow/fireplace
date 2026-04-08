use serde::Serialize;

#[derive(Serialize)]
pub struct SystemNotification {
    pub title: String,
    pub content: String,
}

#[derive(Clone)]
pub struct AppState {
    pub dummy_db_flag: bool,
}
