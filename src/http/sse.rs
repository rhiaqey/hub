use crate::http::state::SharedState;
use crate::hub::metrics::TOTAL_CLIENTS;
use axum::extract::Query;
use log::{debug, info};
use serde::Deserialize;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Deserialize)]
pub struct Params {
    channels: String,
}

pub async fn sse_handler(Query(params): Query<Params>, state: Arc<SharedState>) {
    info!("[GET] Handle SSE connection");
    handle_ws_connection(
        params.channels.split(",").map(|x| x.to_string()).collect(),
        state,
    )
    .await;
}

/// Handle each websocket connection here
async fn handle_ws_connection(channels: Vec<String>, state: Arc<SharedState>) {
    let client_id = Uuid::new_v4();
    info!("connection {client_id} established");
    handle_client(client_id, channels, state).await;
}

/// Handle each client here
async fn handle_client(client_id: Uuid, channels: Vec<String>, state: Arc<SharedState>) {
    info!("handle client {client_id}");
    debug!("channels found {:?}", channels);
    // TODO
    // state.clients.lock().await.insert(client_id, client);
    TOTAL_CLIENTS.set(state.clients.lock().await.len() as f64);
    debug!("client {client_id} was connected")
}
