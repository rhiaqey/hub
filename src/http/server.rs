use crate::http::channels::{assign_channels, create_channels};
use crate::http::state::SharedState;
use axum::routing::{get, post, put};
use axum::Router;
use axum::{http::StatusCode, response::IntoResponse};
use log::trace;
use prometheus::{Encoder, TextEncoder};
use std::sync::Arc;

async fn get_ready() -> impl IntoResponse {
    StatusCode::OK
}

async fn get_metrics() -> impl IntoResponse {
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

async fn get_version() -> &'static str {
    const VERSION: &str = env!("CARGO_PKG_VERSION");
    VERSION
}

pub async fn start_http_server(port: u16, shared_state: Arc<SharedState>) -> hyper::Result<()> {
    let app = Router::new()
        .route("/alive", get(get_ready))
        .route("/ready", get(get_ready))
        .route("/metrics", get(get_metrics))
        .route("/version", get(get_version))
        .route(
            "/admin/channels",
            put({
                let shared_state = Arc::clone(&shared_state);
                move |body| create_channels(body, shared_state)
            }),
        )
        .route(
            "/admin/channels/assign",
            post({
                let shared_state = Arc::clone(&shared_state);
                move |body| assign_channels(body, shared_state)
            }),
        );

    trace!("running http server @ 0.0.0.0:{}", port);

    axum::Server::bind(&format!("0.0.0.0:{}", port).parse().unwrap())
        .serve(app.into_make_service())
        .await
}
