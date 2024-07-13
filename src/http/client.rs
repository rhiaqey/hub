use crate::http::state::SharedState;
use crate::hub::metrics::TOTAL_CLIENTS;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde_json::json;
use std::sync::Arc;

pub async fn get_users_handler(State(state): State<Arc<SharedState>>) -> impl IntoResponse {
    let clients = match TOTAL_CLIENTS.get() {
        None => 0i64,
        Some(g) => g.get(),
    };

    (
        StatusCode::OK,
        [(hyper::header::CONTENT_TYPE, "application/json")],
        json!({
            "Hub": state.get_id(),
            "Clients": clients,
        })
        .to_string(),
    )
}
