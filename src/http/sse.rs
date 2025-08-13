use crate::http::common::{
    get_channel_snapshot_for_client, notify_system_for_client_connect, notify_system_for_client_disconnect, prepare_channels, prepare_client_channel_subscription_messages, prepare_client_connection_message
};
use crate::http::state::SharedState;
use crate::hub::client::sse::SSEClient;
use crate::hub::client::HubClient;
use crate::hub::metrics::TOTAL_CLIENTS;
use crate::hub::simple_channel::SimpleChannels;
use axum::extract::{Query, State};
use axum::response::sse::{Event, KeepAlive};
use axum::response::Sse;
use axum_client_ip::ClientIp;
use axum_extra::{headers, TypedHeader};
use futures::Stream;
use log::{debug, info, trace, warn};
use rhiaqey_sdk_rs::channel::Channel;
use rusty_ulid::generate_ulid_string;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::common::{Params, SnapshotParam};

struct SSEGuard {
    client_id: String,
    state: Arc<SharedState>,
    channels: Vec<(Channel, Option<String>, Option<String>)>,
}

impl Drop for SSEGuard {
    fn drop(&mut self) {
        // 7. disconnect client and remove

        debug!("removing client connection");

        let cid = self.client_id.clone();
        let namespace = self.state.get_namespace().to_owned();
        let redis_rs = self.state.redis_rs.clone();
        let streams = self.state.streams.clone();
        let sse_clients = self.state.clients.clone();
        let channels = self.channels.clone();

        tokio::spawn(async move {
            if let Some(client) = sse_clients.lock().await.remove(&cid) {
                trace!("client was removed from all hub clients");

                // 7.1 remove from channels

                for channel in client.get_channels().iter() {
                    trace!("removing client from {} channel as well", channel.0.name);
                    if let Some(sc) = streams.lock().await.get_mut(&channel.0.name.to_string()) {
                        sc.remove_client(cid.clone());
                    }
                }

                // 7.2 notify for client disconnect

                match notify_system_for_client_disconnect(
                    client.get_client_id(),
                    client.get_user_id(),
                    &namespace,
                    &channels,
                    redis_rs
                ) {
                    Ok(_) => debug!("system notified successfully for client disconnected event"),
                    Err(err) => warn!(
                        "failed to notify system for client disconnected event: {}",
                        err
                    ),
                }
            }

            {
                let total = sse_clients.lock().await.len() as i64;
                TOTAL_CLIENTS.set(total);
                debug!("total connected clients: {}", total);
            }
        });
    }
}

pub async fn sse_handler(
    // headers: HeaderMap,
    Query(params): Query<Params>,
    ClientIp(user_ip): ClientIp,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
    State(state): State<Arc<SharedState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    info!("[GET] Handle sse connection");

    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
        user_agent.to_string()
    } else {
        String::from("Unknown browser")
    };

    debug!("`{}` at {} connected.", user_agent, user_ip);

    let channels = SimpleChannels::from(params.channels.split(",").collect::<Vec<_>>());
    trace!("channel from params extracted {:?}", channels);

    let snapshot_request = params.snapshot.unwrap_or_default();
    trace!("snapshot request: {:?}", snapshot_request);

    let user_id = params.user_id;
    trace!("user id: {:?}", user_id);

    Sse::new(
        handle_sse_client(
            user_ip.to_string(),
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

/// Handle each sse client here
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
    info!("handle sse client {}", &client_id);

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

    let cid = client.get_client_id().clone();
    let hid = client.get_hub_id().clone();
    let mut rx = state.sse_receiver.lock().await.resubscribe();

    let stream = async_stream::stream! {
        // 0. create guard

        let _guard = SSEGuard {
            client_id: cid.clone(),
            state: state.clone(),
            channels: channels.clone(),
        };

        // 1. send client connect message

        match prepare_client_connection_message(&cid, &hid) {
            Ok(message) => match message.ser_to_json_str() {
                Ok(raw) => {
                    yield Ok(Event::default().data(raw));
                },
                Err(err) => warn!("failed to serialize to msgpack: {}", err),
            },
            Err(err) => warn!("failed to prepare connection message: {}", err),
        }

        // 2. send channel subscription

        match prepare_client_channel_subscription_messages(client.get_hub_id(), &channels) {
            Ok(messages) => {
                for message in messages {
                    match message.ser_to_json_str() {
                        Ok(raw) => {
                            yield Ok(Event::default().data(raw));
                        },
                        Err(err) => warn!("failed to serialize to msgpack: {}", err),
                    }
                }
            }
            Err(err) => warn!("failed to prepare channel subscription messages: {}", err),
        }

        // 3. send snapshot

        for channel in get_channel_snapshot_for_client(
            &client,
            &channels,
            state.streams.clone(),
            &snapshot_request,
            snapshot_size,
        )
        .await
        {
            trace!("processing snapshot for channel {}", channel.0);
            for client_message in channel.1.iter() {
                match client_message.ser_to_json_str() {
                    Ok(raw) => yield Ok(Event::default().data(raw)),
                    Err(err) => warn!("failed to send snapshot message: {}", err),
                }
            }
        }

        // 4. notify for client connection

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

        // 5. store client

        state.clients.lock().await.insert(client_id.clone(), client);
        debug!("client {client_id} was connected");

        {
            let total = state.clients.lock().await.len() as i64;
            TOTAL_CLIENTS.set(total);
            debug!("total connected clients: {}", total);
        }

        // 6. listen for messages

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

        // 7. disconnect client and remove
        // handle disconnect in guard drop
    };

    stream
}
