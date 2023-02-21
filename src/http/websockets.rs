use crate::http::state::SharedState;
use axum::extract::ws::WebSocket;
use axum::extract::{ConnectInfo, Query, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::{headers, TypedHeader};
use log::debug;
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct Params {
    channels: String,
}

/// The handler for the HTTP request (this gets called when the HTTP GET lands at the start
/// of websocket negotiation). After this completes, the actual switching from HTTP to
/// websocket protocol will occur.
/// This is the last point where we can extract TCP/IP metadata such as IP address of the client
/// as well as things from HTTP headers such as user-agent of the browser etc.
pub async fn ws_handler(
    Query(params): Query<Params>,
    ws: WebSocketUpgrade,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    state: Arc<SharedState>,
) -> impl IntoResponse {
    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
        user_agent.to_string()
    } else {
        String::from("Unknown browser")
    };
    debug!("`{}` at {} connected.", user_agent, addr.to_string());
    debug!("params extracted {:?}", params.channels);
    // finalize the upgrade process by returning upgrade callback.
    // we can customize the callback by sending additional info such as address.
    ws.on_upgrade(move |socket| handle_socket(socket, addr))
}

/// Actual websocket state machine (one will be spawned per connection)
async fn handle_socket(mut socket: WebSocket, who: SocketAddr) {
    debug!("handling socket connection");
}
