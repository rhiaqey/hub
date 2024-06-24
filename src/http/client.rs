use crate::http::state::SharedState;
use crate::hub::metrics::TOTAL_CLIENTS;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use log::info;
use serde_json::json;
use std::sync::Arc;

pub async fn get_users_handler(State(state): State<Arc<SharedState>>) -> impl IntoResponse {
    let clients = TOTAL_CLIENTS.get();
    info!("[GET] get total connected users: {}", clients);

    (
        StatusCode::OK,
        [(hyper::header::CONTENT_TYPE, "application/json")],
        json!({
            "Hub": state.get_id(),
            "Clients": clients as i64
        })
        .to_string(),
    )
}
