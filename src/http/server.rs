use crate::http::channels::{assign_channels, create_channels, delete_channels};
use crate::http::settings::update_settings;
use crate::http::state::SharedState;
use crate::http::websockets::ws_handler;
use crate::hub::settings::HubSettingsApiKey;
use axum::extract::Query;
use axum::routing::{delete, get, post, put};
use axum::Router;
use axum::{http::StatusCode, response::IntoResponse};
use log::info;
use prometheus::{Encoder, TextEncoder};
use serde::Deserialize;
use sha256::digest;
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

#[derive(Debug, Deserialize)]
struct AuthenticationQueryParams {
    pub api_key: Option<String>,
}

async fn get_auth(
    query: Query<AuthenticationQueryParams>,
    state: Arc<SharedState>,
) -> impl IntoResponse {
    info!("authenticating against api_key");

    if query.api_key.is_none() {
        return (StatusCode::UNAUTHORIZED, "Unauthorized access key");
    }

    let key = query.api_key.clone().unwrap();
    let api_key = HubSettingsApiKey { key: digest(key) };
    let settings = state.settings.lock().await;

    if settings.api_keys.contains(&api_key) {
        (StatusCode::OK, "OK")
    } else {
        (StatusCode::UNAUTHORIZED, "Unauthorized access key")
    }
}

pub async fn start_private_http_server(
    port: u16,
    shared_state: Arc<SharedState>,
) -> hyper::Result<()> {
    let app = Router::new()
        .route("/alive", get(get_ready))
        .route("/ready", get(get_ready))
        .route("/metrics", get(get_metrics))
        .route("/version", get(get_version))
        .route(
            "/auth",
            get({
                let shared_state = Arc::clone(&shared_state);
                move |body| get_auth(body, shared_state)
            }),
        )
        .route(
            "/admin/channels",
            put({
                let shared_state = Arc::clone(&shared_state);
                move |body| create_channels(body, shared_state)
            }),
        )
        .route(
            "/admin/channels",
            delete({
                let shared_state = Arc::clone(&shared_state);
                move |body| delete_channels(body, shared_state)
            }),
        )
        .route(
            "/admin/channels/assign",
            post({
                let shared_state = Arc::clone(&shared_state);
                move |body| assign_channels(body, shared_state)
            }),
        )
        .route(
            "/admin/settings/update",
            post({
                let shared_state = Arc::clone(&shared_state);
                move |body| update_settings(body, shared_state)
            }),
        );

    info!("running private http server @ 0.0.0.0:{}", port);

    axum::Server::bind(&format!("0.0.0.0:{}", port).parse().unwrap())
        .serve(app.into_make_service())
        .await
}

async fn get_home() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

pub async fn start_public_http_server(
    port: u16,
    shared_state: Arc<SharedState>,
) -> hyper::Result<()> {
    let app = Router::new().route("/", get(get_home)).route(
        "/ws",
        get({
            let shared_state = Arc::clone(&shared_state);
            move |query_params, ws, user_agent, info| {
                ws_handler(query_params, ws, user_agent, info, shared_state)
            }
        }),
    );

    info!("running public http server @ 0.0.0.0:{}", port);

    axum::Server::bind(&format!("0.0.0.0:{}", port).parse().unwrap())
        .serve(app.into_make_service_with_connect_info::<SocketAddr>())
        .await
}
