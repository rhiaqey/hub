use crate::http::auth::{get_auth, get_status};
use crate::http::channels::{
    assign_channels, create_channels, delete_channels, get_channel_assignments, get_channels,
    get_publishers,
};
use crate::http::settings::update_settings;
use crate::http::state::SharedState;
use crate::http::websockets::ws_handler;
use axum::routing::{delete, get, post, put};
use axum::Router;
use axum::{http::StatusCode, response::IntoResponse};
use axum_client_ip::SecureClientIpSource;
use log::info;
use prometheus::{Encoder, TextEncoder};
use std::net::SocketAddr;
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
    env!("CARGO_PKG_VERSION")
}

pub async fn start_private_http_server(port: u16, shared_state: Arc<SharedState>) {
    let app = Router::new()
        .route("/alive", get(get_ready))
        .route("/ready", get(get_ready))
        .route("/metrics", get(get_metrics))
        .route("/version", get(get_version))
        .route("/auth", get(get_auth))
        .route("/admin/api/status", get(get_status))
        .route("/admin/api/channels", get(get_channels))
        .route("/admin/api/channels", put(create_channels))
        .route("/admin/api/channels", delete(delete_channels))
        .route("/admin/api/publishers", get(get_publishers))
        .route("/admin/api/channels/assign", post(assign_channels))
        .route("/admin/api/channels/assign", get(get_channel_assignments))
        .route("/admin/api/settings", post(update_settings))
        // .layer(CookieManagerLayer::new())
        .layer(SecureClientIpSource::ConnectInfo.into_extension())
        .with_state(shared_state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    info!(
        "running private http server @ {}",
        listener.local_addr().unwrap()
    );

    axum::serve(
        listener,
        // app.into_make_service()
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .unwrap();
}

pub async fn start_public_http_server(port: u16, shared_state: Arc<SharedState>) {
    let app = Router::new()
        .route("/", get(get_home))
        .route("/ws", get(ws_handler))
        .with_state(shared_state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    info!(
        "running public http server @ {}",
        listener.local_addr().unwrap()
    );

    axum::serve(
        listener,
        // app.into_make_service()
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .unwrap();
}

async fn get_home() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}
