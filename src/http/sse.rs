use crate::http::common::prepare_channels;
use crate::http::state::SharedState;
use crate::hub::client::sse::SSEClient;
use crate::hub::client::HubClient;
use crate::hub::simple_channel::SimpleChannels;
use axum::extract::{Query, State};
use axum::response::sse::{Event, KeepAlive};
use axum::response::Sse;
use axum_client_ip::InsecureClientIp;
use axum_extra::{headers, TypedHeader};
use futures::Stream;
use log::{debug, info, trace, warn};
use rusty_ulid::generate_ulid_string;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::common::{Params, SnapshotParam};

pub async fn sse_handler(
    // headers: HeaderMap,
    Query(params): Query<Params>,
    insecure_ip: InsecureClientIp,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
    State(state): State<Arc<SharedState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    info!("[GET] Handle sse connection");

    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
        user_agent.to_string()
    } else {
        String::from("Unknown browser")
    };

    let ip = insecure_ip.0.to_string();
    debug!("`{}` at {} connected.", user_agent, ip);

    let channels = SimpleChannels::from(params.channels.split(",").collect::<Vec<_>>());
    trace!("channel from params extracted {:?}", channels);

    let snapshot_request = params.snapshot.unwrap_or_default();
    trace!("snapshot request: {:?}", snapshot_request);

    let user_id = params.user_id;
    trace!("user id: {:?}", user_id);

    Sse::new(
        handle_sse_client(
            ip,
            user_id,
            channels,
            snapshot_request,
            params.snapshot_size,
            state,
        )
        .await,
    )
    .keep_alive(KeepAlive::default())
}

/// Handle each client here
async fn handle_sse_client(
    ip: String,
    user_id: Option<String>,
    channels: SimpleChannels,
    snapshot_request: SnapshotParam,
    snapshot_size: Option<usize>,
    state: Arc<SharedState>,
) -> impl Stream<Item = Result<Event, Infallible>> {
    info!("connection {ip} established");

    let client_id = generate_ulid_string();
    info!("handle ws client {}", &client_id);

    let channels = prepare_channels(&client_id, channels, state.streams.clone()).await;
    debug!("{} channels extracted", channels.len());

    let sender = state.sse_sender.lock().await.clone();
    let sx = Arc::new(Mutex::new(sender));
    let client = HubClient::SSE(
        SSEClient::create(
            state.get_id().to_string(),
            client_id.clone(),
            user_id.clone(),
            sx.clone(),
            channels.clone(),
        )
        .unwrap(),
    );

    let mut rx = state.sse_receiver.lock().await.resubscribe();

    let stream = async_stream::stream! {

        loop {
            match rx.recv().await {
                Ok(data) => {
                    yield Ok(Event::default().data(data));
                }
                Err(err) => {
                    warn!("error received {:?}", err);
                    // If the broadcast channel is closed, end the stream
                    break;
                }
            }
        }
    };

    stream
}
