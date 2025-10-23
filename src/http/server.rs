use crate::http::auth::get_auth_handler;
use crate::http::channels::{
    assign_channels_handler, create_channels_handler, delete_channels_handler,
    get_channel_assignments_handler, get_channels_handler, get_hub_handler, get_publishers_handler,
    get_snapshot_handler, purge_channel_handler,
};
use crate::http::metrics::get_metrics_handler;
use crate::http::settings::{update_hub_settings_handler, update_publishers_settings_handler};
use crate::http::sse::sse_handler;
use crate::http::state::SharedState;
use crate::http::websocket::ws_handler;
use axum::http::Method;
use axum::routing::{delete, get, post, put};
use axum::Router;
use axum::{http::StatusCode, response::IntoResponse};
use axum_client_ip::ClientIpSource;
use log::info;
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};

async fn get_ready_handler() -> impl IntoResponse {
    StatusCode::OK
}

async fn get_version_handler() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

pub async fn get_admin_handler() -> impl IntoResponse {
    let admin = include_str!("../../html/index.html");

    (
        StatusCode::OK,
        [(hyper::header::CONTENT_TYPE, "text/html")],
        admin,
    )
}

pub async fn start_private_http_server(port: u16, shared_state: Arc<SharedState>) {
    let app = Router::new()
        .route("/alive", get(get_ready_handler))
        .route("/ready", get(get_ready_handler))
        .route("/metrics", get(get_metrics_handler))
        .route("/version", get(get_version_handler))
        .route("/auth", get(get_auth_handler))
        .route("/admin", get(get_admin_handler))
        .route("/admin/api/hub", get(get_hub_handler))
        .route("/admin/api/channels/:channel", delete(purge_channel_handler))
        .route("/admin/api/channels", get(get_channels_handler))
        .route("/admin/api/channels", put(create_channels_handler))
        .route("/admin/api/channels", delete(delete_channels_handler))
        .route("/admin/api/publishers", get(get_publishers_handler))
        .route("/admin/api/channels/assign", post(assign_channels_handler))
        .route(
            "/admin/api/channels/assign",
            get(get_channel_assignments_handler),
        )
        .route("/admin/api/hub/settings", post(update_hub_settings_handler))
        .route(
            "/admin/api/publishers/settings",
            post(update_publishers_settings_handler),
        )
        .layer(ClientIpSource::XRealIp.into_extension())
        .with_state(shared_state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    info!(
        "running private http server @ {}",
        listener.local_addr().unwrap()
    );

    info!(
        "admin panel available @ http://{}/admin",
        listener.local_addr().unwrap()
    );

    info!(
        "metrics panel available @ http://{}/metrics",
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
    let cors = CorsLayer::new()
        .allow_methods([Method::GET])
        .allow_origin(Any);

    let app = Router::new()
        .route("/", get(get_home_handler))
        .route("/ws", get(ws_handler))
        .route("/sse", get(sse_handler))
        .route("/snapshot", get(get_snapshot_handler))
        .layer(cors)
        .layer(ClientIpSource::XRealIp.into_extension())
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

async fn get_home_handler() -> impl IntoResponse {
    info!("[GET] Handle home");
    (StatusCode::OK, "OK")
}
