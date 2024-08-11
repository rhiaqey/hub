
use tokio::sync::Mutex;
use axum::{extract::{Query, State}, response::{sse::{Event, KeepAlive}, Sse}};
use axum_client_ip::InsecureClientIp;
use axum_extra::{headers, TypedHeader};
use futures::{pin_mut, Stream, TryStreamExt};
use log::{debug, info, trace, warn};
use rusty_ulid::generate_ulid_string;
use std::{convert::Infallible, pin::{self, Pin}, sync::Arc, time::Duration};
// use tokio_stream::{wrappers::BroadcastStream, Stream, StreamExt};
use async_stream::stream;
use futures::future::join_all;
use futures::stream::repeat_with;
use tokio_stream::StreamExt as _ ;
use futures_util::stream::{self};


use crate::{http::common::prepare_channels, hub::{simple_channel::SimpleChannels, sse_client::SSEClient}};

use super::{state::SharedState, websocket::{Params, SnapshotParam}};

async fn handle_sse_client(
    ip: String,
    user_id: Option<String>,
    channels: SimpleChannels,
    snapshot_request: SnapshotParam,
    snapshot_size: Option<usize>,
    state: Arc<SharedState>,
) -> impl Stream<Item = Result<Event, Infallible>> {
    let client_id = generate_ulid_string();
    info!("handle ws client {}", &client_id);

    let channels = prepare_channels(&client_id, channels, state.streams.clone()).await;
    debug!("{} channels extracted", channels.len());

    let sender = state.sse_sender.lock().await.clone();
    let sx = Arc::new(Mutex::new(sender));
    let mut client = SSEClient::create(
        state.get_id().to_string(),
        client_id.clone(),
        user_id.clone(),
        sx.clone(),
        channels.clone(),
    ).unwrap();
    
    struct Guard {
        // whatever state you need here
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            warn!("stream closed");
        }
    }

    let mut rx = state.sse_receiver.lock().await.resubscribe(); 

    let ping = async_stream::stream! {
        let _guard = Guard{};

        loop {
            match rx.recv().await {
                Ok(msg) => {
                    yield Ok(Event::default().data(msg));
                }
                Err(err) => {
                    warn!("error received {:?}", err);
                    // If the broadcast channel is closed, end the stream
                    break;
                }
            }
        }

        // `_guard` is dropped
    };

    ping
}

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

    let stream = handle_sse_client(ip, user_id, channels, snapshot_request, params.snapshot_size, state).await;

    Sse::new(stream).keep_alive(KeepAlive::default())
}
