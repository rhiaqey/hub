use crate::http::common::{prepare_channels, prepare_client_channel_subscription_messages, prepare_client_connection_message};
use crate::http::state::SharedState;
use crate::hub::websocket_client::WebSocketClient;
use crate::hub::metrics::TOTAL_CLIENTS;
use crate::hub::simple_channel::SimpleChannels;
use crate::hub::streaming_channel::StreamingChannel;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Query, State, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum_client_ip::InsecureClientIp;
use axum_extra::{headers, TypedHeader};
use futures::{SinkExt, StreamExt};
use log::{debug, info, trace, warn};
use redis::Commands;
use rhiaqey_common::client::ClientMessage;
use rhiaqey_common::pubsub::{
    ClientConnectedMessage, ClientDisconnectedMessage, RPCMessage, RPCMessageData,
};
use rhiaqey_common::topics;
use rhiaqey_sdk_rs::channel::Channel;
use rusty_ulid::generate_ulid_string;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::http::common::SnapshotParam;

#[derive(Deserialize)]
pub struct Params {
    pub(crate) channels: String,
    pub(crate) snapshot: Option<SnapshotParam>,
    pub(crate) snapshot_size: Option<usize>,
    pub(crate) user_id: Option<String>,
}

/// The handler for the HTTP request (this gets called when the HTTP GET lands at the start
/// of websocket negotiation). After this completes, the actual switching from HTTP to
/// websocket protocol will occur.
/// This is the last point where we can extract TCP/IP metadata such as IP address of the client
/// as well as things from HTTP headers such as user-agent of the browser, etc.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    // headers: HeaderMap,
    Query(params): Query<Params>,
    insecure_ip: InsecureClientIp,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
    State(state): State<Arc<SharedState>>,
) -> impl IntoResponse {
    info!("[GET] Handle websocket connection");

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

    // finalize the upgrade process by returning upgrade callback.
    // we can customize the callback by sending additional info such as address.
    ws.on_upgrade(move |socket| {
        handle_ws_connection(
            socket,
            ip,
            user_id,
            channels,
            snapshot_request,
            params.snapshot_size,
            state,
        )
    })
}

/// Handle each websocket connection here
async fn handle_ws_connection(
    socket: WebSocket,
    ip: String,
    user_id: Option<String>,
    channels: SimpleChannels,
    snapshot_request: SnapshotParam,
    snapshot_size: Option<usize>,
    state: Arc<SharedState>,
) {
    info!("connection {ip} established");
    tokio::task::spawn(async move {
        handle_ws_client(
            socket,
            user_id,
            channels,
            snapshot_request,
            snapshot_size,
            state,
        )
        .await
    });
}

#[inline(always)]
async fn send_snapshot_to_client(
    client: &mut WebSocketClient,
    channels: &Vec<(Channel, Option<String>, Option<String>)>,
    streams: Arc<Mutex<HashMap<String, StreamingChannel>>>,
    snapshot_request: &SnapshotParam,
    snapshot_size: Option<usize>,
) -> anyhow::Result<()> {
    let client_id = client.get_client_id().clone();
    let mut lock = streams.lock().await;

    for channel in channels.iter() {
        let channel_name = channel.0.name.clone();
        let streaming_channel = lock.get_mut(&*channel_name);
        if let Some(chx) = streaming_channel {
            if snapshot_request.allowed() {
                // send snapshot

                let snapshot = chx
                    .get_snapshot(
                        snapshot_request,
                        channel.1.clone(),
                        channel.2.clone(),
                        snapshot_size,
                    )
                    .unwrap_or(vec![]);

                for stream_message in snapshot.iter() {
                    // case where clients have specified a category for their channel
                    if channel.1.is_some() {
                        if !stream_message.category.eq(&channel.1) {
                            warn!(
                                "snapshot category {:?} does not match with specified {:?}",
                                stream_message.category, channel.1
                            );
                            continue;
                        }
                    }

                    let mut client_message = ClientMessage::from(stream_message);

                    if cfg!(debug_assertions) {
                        if client_message.hub_id.is_none() {
                            client_message.hub_id = Some(client.get_hub_id().to_string());
                        }
                    } else {
                        client_message.hub_id = None;
                        client_message.publisher_id = None;
                    }

                    let raw = rmp_serde::to_vec_named(&client_message).unwrap();
                    if let Ok(_) = client.send(Message::Binary(raw)).await {
                        trace!(
                            "channel snapshot message[category={:?}] sent successfully to {}",
                            channel.1,
                            &client_id
                        );
                    } else {
                        warn!("could not send snapshot message to {}", &client_id);
                        continue;
                    }
                }
            } else {
                // send only the latest message
                if let Some(raw) = chx.get_last_client_message(channel.1.clone()) {
                    trace!("sending last channel message instead");
                    if let Ok(_) = client.send(Message::Binary(raw)).await {
                        trace!(
                            "last channel message[category={:?}] sent successfully to {}",
                            channel.1,
                            &client_id
                        );
                    } else {
                        warn!("could not send last channel message to {}", &client_id);
                        continue;
                    }
                }
            }
        }
    }

    Ok(())
}

#[inline(always)]
fn notify_system_for_client_connect(
    client: &WebSocketClient,
    namespace: &str,
    channels: &Vec<(Channel, Option<String>, Option<String>)>,
    redis: Arc<std::sync::Mutex<redis::Connection>>,
) -> anyhow::Result<()> {
    let raw = serde_json::to_vec(&RPCMessage {
        data: RPCMessageData::ClientConnected(ClientConnectedMessage {
            client_id: client.get_client_id().clone(),
            user_id: client.get_user_id().clone(),
            channels: channels.clone(),
        }),
    })?;

    let event_topic = topics::events_pubsub_topic(namespace);

    let _: () = redis
        .lock()
        .unwrap()
        .publish(&event_topic, raw)
        .expect("failed to publish message");

    debug!("event sent for client connect to {}", &event_topic);

    Ok(())
}

#[inline(always)]
fn notify_system_for_client_disconnect(
    client: &WebSocketClient,
    namespace: &str,
    channels: &Vec<(Channel, Option<String>, Option<String>)>,
    redis: Arc<std::sync::Mutex<redis::Connection>>,
) -> anyhow::Result<()> {
    let raw = serde_json::to_vec(&RPCMessage {
        data: RPCMessageData::ClientDisconnected(ClientDisconnectedMessage {
            client_id: client.get_client_id().clone(),
            user_id: client.get_user_id().clone(),
            channels: channels.clone(),
        }),
    })?;

    let event_topic = topics::events_pubsub_topic(namespace);

    let _: () = redis
        .lock()
        .unwrap()
        .publish(&event_topic, raw)
        .expect("failed to publish message");

    debug!("event sent for client disconnect to {}", &event_topic);

    Ok(())
}

/// Handle each client here
async fn handle_ws_client(
    socket: WebSocket,
    user_id: Option<String>,
    channels: SimpleChannels,
    snapshot_request: SnapshotParam,
    snapshot_size: Option<usize>,
    state: Arc<SharedState>,
) {
    let client_id = generate_ulid_string();
    info!("handle ws client {}", &client_id);

    let channels = prepare_channels(&client_id, channels, state.streams.clone()).await;
    debug!("{} channels extracted", channels.len());

    let (sender, mut receiver) = socket.split();
    let sx = Arc::new(Mutex::new(sender));
    let mut client = WebSocketClient::create(
        state.get_id().to_string(),
        client_id.clone(),
        user_id.clone(),
        sx.clone(),
        channels.clone(),
    ).unwrap();    

    match prepare_client_connection_message(client.get_client_id(), client.get_hub_id()) {
        Ok(message) => match message.ser_to_binary() {
            Ok(data) => match client.send(Message::Binary(data)).await {
                Ok(_) => debug!("client connection message sent successfully"),
                Err(err) => warn!("failed to send client message: {}", err),
            },
            Err(err) => warn!("failed to serialize connection message to binary: {}", err),
        },
        Err(err) => warn!("failed to prepare connection message: {}", err),
    }

    match prepare_client_channel_subscription_messages(client.get_hub_id(), &channels) {
        Ok(messages) => {
            for message in messages {
                match message.ser_to_binary() {
                    Ok(data) => match client.send(Message::Binary(data)).await {
                        Ok(_) => debug!("client channel subscription message sent successfully"),
                        Err(err) => warn!("failed to send client message: {}", err),
                    },
                    Err(err) => warn!("failed to serialize client channel subscription message to binary: {}", err),
                }
            }
        }
        Err(err) => warn!("failed to prepare channel subscription messages: {}", err),
    }

    match send_snapshot_to_client(
        &mut client,
        &channels,
        state.streams.clone(),
        &snapshot_request,
        snapshot_size,
    )
    .await
    {
        Ok(_) => debug!("client channel subscription message sent successfully"),
        Err(err) => warn!("failed to send channel snapshot to client: {}", err),
    }

    match notify_system_for_client_connect(
        &mut client,
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

    let cid = client.get_client_id().clone();
    state.websocket_clients.lock().await.insert(client_id.clone(), client);
    let total = state.websocket_clients.lock().await.len() as i64;
    TOTAL_CLIENTS.set(total);

    debug!("client {client_id} was connected");
    debug!("total connected clients: {}", total);

    while let Some(Ok(msg)) = receiver.next().await {
        match msg {
            Message::Ping(_) => {}
            Message::Pong(_) => {}
            Message::Close(_) | Message::Text(_) | Message::Binary(_) => {
                warn!("message received from connected client {}", &client_id);
                debug!("received message {:?}", msg);

                if let Err(err) = sx.lock().await.close().await {
                    warn!("error closing connection for {}: {err}", &client_id);
                } else {
                    debug!("client {} connection closed", &client_id)
                }

                break;
            }
        }
    }

    debug!("removing client connection");

    if let Some(client) = state.websocket_clients.lock().await.remove(&cid) {
        trace!("client was removed from all hub clients");

        for channel in client.channels.iter() {
            trace!("removing client from {} channel as well", channel.0.name);
            if let Some(sc) = state
                .streams
                .lock()
                .await
                .get_mut(&channel.0.name.to_string())
            {
                sc.remove_client(cid.clone());
            }
        }
    }

    if let Some(mut client) = state.websocket_clients.lock().await.remove(&cid) {
        match notify_system_for_client_disconnect(
            &mut client,
            state.get_namespace(),
            &channels,
            state.redis_rs.clone(),
        ) {
            Ok(_) => debug!("system notified successfully for client disconnected event"),
            Err(err) => warn!(
                "failed to notify system for client disconnected event: {}",
                err
            ),
        }
    }

    let total_clients = state.websocket_clients.lock().await.len() as i64;
    TOTAL_CLIENTS.set(total_clients);
    debug!("total connected clients: {}", total_clients);
}
