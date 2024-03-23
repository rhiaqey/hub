use crate::hub::metrics::TOTAL_CLIENTS;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use log::info;
use serde_json::json;

pub async fn get_users() -> impl IntoResponse {
    info!("[GET] get total connected users");
    let clients = TOTAL_CLIENTS.get();

    (
        StatusCode::OK,
        [(hyper::header::CONTENT_TYPE, "application/json")],
        json!({
            "Clients": clients as i64
        })
        .to_string(),
    )
}
