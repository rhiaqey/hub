use crate::http::state::SharedState;
use crate::hub::metrics::{SSE_TOTAL_CLIENTS, WS_TOTAL_CLIENTS};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde_json::json;
use std::sync::Arc;

pub async fn get_users_handler(State(state): State<Arc<SharedState>>) -> impl IntoResponse {
    let ws_clients = WS_TOTAL_CLIENTS.get();
    let sse_clients = SSE_TOTAL_CLIENTS.get();

    (
        StatusCode::OK,
        [(hyper::header::CONTENT_TYPE, "application/json")],
        json!({
            "Hub": state.get_id(),
            "Clients": ws_clients + sse_clients,
            "WS_Clients": ws_clients,
            "SSE_Clients": sse_clients,
        })
        .to_string(),
    )
}
