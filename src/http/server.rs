use crate::http::auth::{get_auth_handler, get_status_handler};
use crate::http::channels::{
    assign_channels_handler, create_channels_handler, delete_channels_handler,
    get_channel_assignments_handler, get_channels_handler, get_hub_handler, get_publishers_handler,
    get_snapshot_handler, purge_channel_handler,
};
use crate::http::client::get_users_handler;
use crate::http::metrics::get_metrics_handler;
use crate::http::settings::update_settings_handler;
use crate::http::state::SharedState;
use crate::http::websocket::ws_handler;
use axum::http::Method;
use axum::routing::{delete, get, post, put};
use axum::Router;
use axum::{http::StatusCode, response::IntoResponse};
use axum_client_ip::SecureClientIpSource;
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
        .route("/admin/", get(get_admin_handler))
        .route("/admin/api/hub", get(get_hub_handler))
        .route("/admin/api/status", get(get_status_handler))
        .route("/admin/api/users", get(get_users_handler))
        .route("/admin/api/channel/:channel", delete(purge_channel_handler))
        .route("/admin/api/channels", get(get_channels_handler))
        .route("/admin/api/channels", put(create_channels_handler))
        .route("/admin/api/channels", delete(delete_channels_handler))
        .route("/admin/api/publishers", get(get_publishers_handler))
        .route("/admin/api/channels/assign", post(assign_channels_handler))
        .route(
            "/admin/api/channels/assign",
            get(get_channel_assignments_handler),
        )
        .route("/admin/api/settings", post(update_settings_handler))
        // .layer(CookieManagerLayer::new())
        .layer(SecureClientIpSource::ConnectInfo.into_extension())
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
        .route("/snapshot", get(get_snapshot_handler))
        .layer(cors)
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
