use axum::http::StatusCode;
use axum::response::IntoResponse;
use log::info;
use prometheus::{Encoder, TextEncoder};

pub async fn get_metrics_handler() -> impl IntoResponse {
    info!("[GET] Get metrics");

    let encoder = TextEncoder::new();
    let mut buffer = vec![];
    let mf = prometheus::gather();
    encoder.encode(&mf, &mut buffer).unwrap();
    (
        StatusCode::OK,
        [(
            hyper::header::CONTENT_TYPE,
            encoder.format_type().to_string(),
        )],
        buffer.into_response(),
    )
}
