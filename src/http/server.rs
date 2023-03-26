use crate::http::channels::{assign_channels, create_channels, delete_channels};
use crate::http::settings::update_settings;
use crate::http::state::SharedState;
use crate::http::websockets::ws_handler;
use crate::hub::settings::HubSettingsApiKey;
use axum::extract::Query;
use axum::http::HeaderMap;
use axum::routing::{delete, get, post, put};
use axum::Router;
use axum::{http::StatusCode, response::IntoResponse};
use log::info;
use prometheus::{Encoder, TextEncoder};
use serde::Deserialize;
use sha256::digest;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use url::Url;

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

async fn valid_api_key(key: String, state: Arc<SharedState>) -> bool {
    let api_key = HubSettingsApiKey { key: digest(key) };
    let settings = state.settings.lock().await;
    settings.api_keys.contains(&api_key)
}

fn extract_api_key(relative_path: &str) -> Option<String> {
    let full = format!("http://localhost{}", relative_path);

    if let Ok(parts) = Url::parse(full.as_str()) {
        let queries: HashMap<_, _> = parts.query_pairs().collect();
        if queries.contains_key("api_key") {
            let api_key = queries.get("api_key").unwrap().to_string();
            return Some(api_key);
        }
    }

    None
}

async fn get_auth(
    headers: HeaderMap,
    query: Query<AuthenticationQueryParams>,
    state: Arc<SharedState>,
) -> impl IntoResponse {
    info!("authenticating with api_key");

    if query.api_key.is_some() {
        info!("query api key found");

        return if valid_api_key(query.api_key.clone().unwrap(), state).await {
            (StatusCode::OK, "OK")
        } else {
            (StatusCode::UNAUTHORIZED, "Unauthorized access")
        };
    }

    if headers.contains_key("x-forwarded-uri") {
        info!("x-forwarded-uri found");

        let path = headers.get("x-forwarded-uri").unwrap().to_str().unwrap();
        if let Some(api_key) = extract_api_key(path) {
            if valid_api_key(api_key, state).await {
                return (StatusCode::OK, "OK");
            }
        }
    }

    (StatusCode::UNAUTHORIZED, "Unauthorized access")
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
                move |headers, query| get_auth(headers, query, shared_state)
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
