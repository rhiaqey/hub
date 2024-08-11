use axum::{
    extract::{Query, State},
    response::{
        sse::{Event, KeepAlive},
        Sse,
    },
};
use axum_client_ip::InsecureClientIp;
use axum_extra::{headers, TypedHeader};
use futures::{pin_mut, Stream, TryStreamExt};
use log::{debug, info, trace, warn};
use rusty_ulid::generate_ulid_string;
use std::{
    convert::Infallible,
    pin::{self, Pin},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Mutex;
// use tokio_stream::{wrappers::BroadcastStream, Stream, StreamExt};
use async_stream::stream;
use futures::future::join_all;
use futures::stream::repeat_with;
use futures_util::stream::{self};
use tokio_stream::StreamExt as _;

use crate::{
    http::common::{
        get_channel_snapshot_for_client, notify_system_for_client_connect, prepare_channels, prepare_client_channel_subscription_messages, prepare_client_connection_message, ChannelSnapshotResult
    },
    hub::{simple_channel::SimpleChannels, sse_client::SSEClient},
};

use super::{common::SnapshotParam, state::SharedState, websocket::Params};

/// Handle each client here
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
    )
    .unwrap();

    struct Guard {
        // whatever state you need here
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            warn!("stream closed");
        }
    }

    let mut rx = state.sse_receiver.lock().await.resubscribe();

    let stream = async_stream::stream! {
        let _guard = Guard{};

        match prepare_client_connection_message(client.get_client_id(), client.get_hub_id()) {
            Ok(message) => match message.ser_to_string() {
                Ok(data) => {
                    yield Ok(Event::default().data(data));
                },
                Err(err) => warn!("failed to serialize connection message to binary: {}", err),
            },
            Err(err) => warn!("failed to prepare connection message: {}", err),
        }

        match prepare_client_channel_subscription_messages(client.get_hub_id(), &channels) {
            Ok(messages) => {
                for message in messages {
                    match message.ser_to_string() {
                        Ok(data) => {
                            yield Ok(Event::default().data(data));
                        },
                        Err(err) => warn!("failed to serialize client channel subscription message to binary: {}", err),
                    }
                }
            }
            Err(err) => warn!("failed to prepare channel subscription messages: {}", err),
        }

        for channel in get_channel_snapshot_for_client(
            client.get_hub_id(),
            &channels,
            state.streams.clone(),
            &snapshot_request,
            snapshot_size
        ).await {
            trace!("processing snapshot for channel {}", channel.0);

            match channel.1 {
                ChannelSnapshotResult::Messages(messages) => {
                    for client_message in messages.iter() {
                        match client_message.ser_to_string() {
                            Ok(raw) => {
                                trace!(
                                    "channel snapshot message[category={:?}] sent successfully to {}",
                                    &channel.0,
                                    &client_id
                                );
                                yield Ok(Event::default().data(raw));
                            },
                            Err(err) => {
                                warn!("failed to serialize to string: {}", err);
                                continue;
                            }
                        }
                    }
                }
                ChannelSnapshotResult::LastMessage(message) => {
                    trace!("last client message found: {:?}", message);
                    match message.ser_to_string() {
                        Ok(raw) => {
                            trace!(
                                "last channel message[category={:?}] sent successfully to {}",
                                &channel.0,
                                &client_id
                            );
                            yield Ok(Event::default().data(raw));
                        },
                        Err(err) => {
                            warn!("failed to serialize to string: {}", err);
                            continue;
                        }
                    }
                }
            }
        }

        match notify_system_for_client_connect(
            client.get_client_id(),
            client.get_user_id(),
            state.get_namespace(),
            &channels,
            state.redis_rs.clone(),
        ) {
            Ok(_) => debug!("system notified successfully for client connected event"),
            Err(err) => warn!(
                "failed to notify system for client connected event: {}",
                err
            ),
        }

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

        // `_guard` is dropped
    };

    stream
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
