use crate::http::auth::{extract_api_key, get_auth};
use crate::http::channels::{assign_channels, create_channels, delete_channels};
use crate::http::settings::update_settings;
use crate::http::state::SharedState;
use crate::http::websockets::ws_handler;
use axum::routing::{delete, get, post, put};
use axum::Router;
use axum::{http::StatusCode, response::IntoResponse};
use http::{request::Parts as RequestParts, HeaderValue};
use log::{debug, info, warn};
use prometheus::{Encoder, TextEncoder};
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::{AllowOrigin, CorsLayer};

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
    let settings = Arc::clone(&shared_state.settings);

    let cors = CorsLayer::new().allow_origin(AllowOrigin::predicate(
        move |origin: &HeaderValue, request_parts: &RequestParts| {
            if let Some(path_and_query) = request_parts.uri.path_and_query() {
                if let Some(_api_key) = extract_api_key(path_and_query.as_str()) {
                    info!("api key found in cors {}", path_and_query);
                } else {
                    warn!("api key was not found near {}", path_and_query);
                }
            }

            let settings = settings.read().unwrap();

            if let Some(domains) = &settings.domains {
                if let Ok(domain) = std::str::from_utf8(origin.as_bytes()) {
                    let contains = domains.contains(&domain.to_string());
                    if contains {
                        debug!("allowing cors domain {}", domain);
                    } else {
                        warn!("cors domain {} is not allowed", domain);
                    }

                    return contains;
                }
            }

            warn!("no whitelisted domains found.");

            debug!("allowing all");

            return true;
        },
    ));

    let app = Router::new()
        .route("/", get(get_home))
        .route(
            "/ws",
            get({
                let shared_state = Arc::clone(&shared_state);
                move |query_params, ws, user_agent, info| {
                    ws_handler(query_params, ws, user_agent, info, shared_state)
                }
            }),
        )
        .layer(cors);

    info!("running public http server @ 0.0.0.0:{}", port);

    axum::Server::bind(&format!("0.0.0.0:{}", port).parse().unwrap())
        .serve(app.into_make_service_with_connect_info::<SocketAddr>())
        .await
}
