use axum::{
    extract::{Query, State},
    response::{
        sse::{Event, KeepAlive},
        Sse,
    },
};
use axum_client_ip::InsecureClientIp;
use axum_extra::{headers, TypedHeader};
use futures::Stream;
use log::{debug, info, trace, warn};
use rhiaqey_sdk_rs::channel::Channel;
use rusty_ulid::generate_ulid_string;
use std::{convert::Infallible, sync::Arc};
use tokio::{sync::Mutex, task::yield_now};

use crate::{
    http::common::{
        get_channel_snapshot_for_client, notify_system_for_client_connect,
        notify_system_for_client_disconnect, prepare_channels,
        prepare_client_channel_subscription_messages, prepare_client_connection_message,
        ChannelSnapshotResult,
    },
    hub::{metrics::SSE_TOTAL_CLIENTS, simple_channel::SimpleChannels, sse_client::SSEClient},
};

use super::{common::SnapshotParam, state::SharedState, websocket::Params};

struct Guard {
    client_id: String,
    state: Arc<SharedState>,
    channels: Vec<(Channel, Option<String>, Option<String>)>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        debug!("removing client connection");

        let cid = self.client_id.clone();
        let namespace = self.state.get_namespace().to_owned();
        let streams = self.state.streams.clone();
        let sse_clients = self.state.clients.clone();
        let redis_rs = self.state.redis_rs.clone();
        let channels = self.channels.clone();

        tokio::spawn(async move {
            if let Some(client) = sse_clients.lock().await.remove(&cid) {
                trace!("client was removed from all hub clients");

                for channel in client.get_channels().iter() {
                    trace!("removing client from {} channel as well", channel.0.name);
                    if let Some(sc) = streams.lock().await.get_mut(&channel.0.name.to_string()) {
                        sc.remove_client(cid.clone());
                    }
                }

                match notify_system_for_client_disconnect(
                    client.get_client_id(),
                    client.get_user_id(),
                    namespace.as_str(),
                    &channels,
                    redis_rs,
                ) {
                    Ok(_) => debug!("system notified successfully for client disconnected event"),
                    Err(err) => warn!(
                        "failed to notify system for client disconnected event: {}",
                        err
                    ),
                }
            }

            let total = sse_clients.lock().await.len() as i64;
            SSE_TOTAL_CLIENTS.set(total);

            debug!("sse client {} was connected", &cid);
            debug!("total sse connected clients: {}", total);

            yield_now().await;
        });
    }
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
    let client = SSEClient::create(
        state.get_id().to_string(),
        client_id.clone(),
        user_id.clone(),
        channels.clone(),
        sx.clone(),
    )
    .unwrap();

    let mut rx = state.sse_receiver.lock().await.resubscribe();

    let stream = async_stream::stream! {
        let _guard = Guard{
            client_id: client.client_id.clone(),
            state: state.clone(),
            channels: channels.clone(),
        };

        match prepare_client_connection_message(&client.client_id, &client.hub_id) {
            Ok(message) => match message.ser_to_string() {
                Ok(data) => {
                    yield Ok(Event::default().data(data));
                },
                Err(err) => warn!("failed to serialize connection message to binary: {}", err),
            },
            Err(err) => warn!("failed to prepare connection message: {}", err),
        }

        match prepare_client_channel_subscription_messages(&client.hub_id, &channels) {
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
            &client.hub_id,
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
            &client.client_id,
            &client.user_id,
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

        state
            .clients
            .lock()
            .await
            .insert(client_id.clone(), crate::http::common::ConnectedClient::SSE(client));
        let total = state.clients.lock().await.len() as i64;
        SSE_TOTAL_CLIENTS.set(total);

        debug!("sse client {client_id} was connected");
        debug!("total sse connected clients: {}", total);

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
        // disconnect is moved to _guard
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
