use crate::http::state::SharedState;
use crate::hub::client::WebSocketClient;
use crate::hub::metrics::TOTAL_CLIENTS;
use std::collections::HashMap;

use crate::hub::simple_channel::SimpleChannels;
use crate::hub::streaming_channel::StreamingChannel;
use anyhow::bail;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Query, State, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum_client_ip::InsecureClientIp;
use axum_extra::{headers, TypedHeader};
use futures::{SinkExt, StreamExt};
use log::{debug, info, trace, warn};
use redis::Commands;
use rhiaqey_common::client::{
    ClientMessage, ClientMessageDataType, ClientMessageValue,
    ClientMessageValueClientChannelSubscription, ClientMessageValueClientConnection,
};
use rhiaqey_common::pubsub::{
    ClientConnectedMessage, ClientDisconnectedMessage, RPCMessage, RPCMessageData,
};
use rhiaqey_common::topics;
use rhiaqey_sdk_rs::channel::Channel;
use rhiaqey_sdk_rs::message::MessageValue;
use rusty_ulid::generate_ulid_string;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotParam {
    ASC,
    DESC,
    TRUE,
    FALSE,
}

impl Default for SnapshotParam {
    fn default() -> Self {
        Self::FALSE
    }
}

impl SnapshotParam {
    fn allowed(&self) -> bool {
        match *self {
            SnapshotParam::ASC => true,
            SnapshotParam::DESC => true,
            SnapshotParam::TRUE => true,
            SnapshotParam::FALSE => false,
        }
    }
}

#[derive(Deserialize)]
pub struct Params {
    channels: String,
    snapshot: Option<SnapshotParam>,
    snapshot_size: Option<usize>,
    user_id: Option<String>,
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
async fn prepare_channels(
    client_id: &String,
    channels: SimpleChannels,
    streams: Arc<Mutex<HashMap<String, StreamingChannel>>>,
) -> Vec<(Channel, Option<String>, Option<String>)> {
    // With this, we will support channel names that can include categories seperated with a `/`
    // Valid examples would be `ticks` but also `ticks/historical`.
    // Any other format would be considered invalid and would be filtered out.
    let channels: Vec<(String, Option<String>, Option<String>)> =
        channels.get_channels_with_category_and_key();

    let mut added_channels: Vec<(Channel, Option<String>, Option<String>)> = vec![];

    {
        for channel in channels.iter() {
            let mut lock = streams.lock().await;
            let streaming_channel = lock.get_mut(&channel.0);
            if let Some(chx) = streaming_channel {
                chx.add_client(client_id.clone());
                added_channels.push((
                    chx.get_channel().clone(),
                    channel.1.clone(),
                    channel.2.clone(),
                ));
                debug!("client joined channel {}", channel.0);
            } else {
                warn!("could not find channel {}", channel.0);
            }
        }
    }

    added_channels
}

#[inline(always)]
fn prepare_client_connection_message(
    client_id: &String,
    hub_id: &String,
) -> anyhow::Result<Vec<u8>> {
    let client_message = ClientMessage {
        data_type: ClientMessageDataType::ClientConnection as u8,
        channel: String::from(""),
        key: String::from(""),
        value: ClientMessageValue::ClientConnection(ClientMessageValueClientConnection {
            client_id: client_id.to_string(),
            hub_id: hub_id.to_string(),
        }),
        tag: None,
        category: None,
        hub_id: Some(hub_id.clone()),
        publisher_id: None,
    };

    match rmp_serde::to_vec(&client_message) {
        Ok(data) => Ok(data),
        Err(err) => bail!(err),
    }
}

#[inline(always)]
fn prepare_client_channel_subscription_messages(
    hub_id: &String,
    channels: &Vec<(Channel, Option<String>, Option<String>)>,
) -> anyhow::Result<Vec<Vec<u8>>> {
    let mut result = vec![];

    let mut data = ClientMessage {
        data_type: ClientMessageDataType::ClientChannelSubscription as u8,
        channel: "".to_string(),
        key: "".to_string(),
        value: ClientMessageValue::Data(MessageValue::Text(String::from(""))),
        tag: None,
        category: None,
        hub_id: Some(hub_id.to_string()),
        publisher_id: None,
    };

    for channel in channels {
        data.channel = channel.0.name.to_string();
        data.key = channel.0.name.to_string();
        data.category = channel.1.clone();
        data.value = ClientMessageValue::ClientChannelSubscription(
            ClientMessageValueClientChannelSubscription {
                channel: Channel {
                    name: channel.0.name.clone(),
                    size: channel.0.size,
                },
            },
        );

        match rmp_serde::to_vec(&data) {
            Ok(raw) => result.push(raw),
            Err(err) => warn!("failed to serialize to vec: {err}"),
        }
    }

    Ok(result)
}

#[inline(always)]
async fn send_snapshot_to_client(
    client: &mut WebSocketClient,
    channels: &Vec<(Channel, Option<String>, Option<String>)>,
    streams: Arc<Mutex<HashMap<String, StreamingChannel>>>,
    snapshot_request: &SnapshotParam,
    snapshot_size: Option<usize>,
) -> anyhow::Result<()> {
    let client_id = client.client_id.clone();
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
                    if client_message.hub_id.is_none() {
                        client_message.hub_id = Some(client.get_hub_id().to_string());
                    }

                    let raw = rmp_serde::to_vec(&client_message).unwrap();
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
    namespace: String,
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
    namespace: String,
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
        state.get_id(),
        client_id.clone(),
        user_id.clone(),
        sx.clone(),
        channels.clone(),
    );

    match prepare_client_connection_message(client.get_client_id(), client.get_hub_id()) {
        Ok(message) => match client.send(Message::Binary(message)).await {
            Ok(_) => debug!("client connection message sent successfully"),
            Err(err) => warn!("failed to send client message: {}", err),
        },
        Err(err) => warn!("failed to prepare connection message: {}", err),
    }

    match prepare_client_channel_subscription_messages(client.get_hub_id(), &channels) {
        Ok(messages) => {
            for message in messages {
                match client.send(Message::Binary(message)).await {
                    Ok(_) => debug!("client channel subscription message sent successfully"),
                    Err(err) => warn!("failed to send client message: {}", err),
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
    state.clients.lock().await.insert(client_id.clone(), client);
    TOTAL_CLIENTS.set(state.clients.lock().await.len() as f64);

    debug!("client {client_id} was connected");
    debug!("total connected clients: {}", TOTAL_CLIENTS.get());

    while let Some(Ok(msg)) = receiver.next().await {
        match msg {
            Message::Ping(_) => {}
            Message::Pong(_) => {}
            Message::Close(_) | Message::Text(_) | Message::Binary(_) => {
                warn!("message received from connected client {}", &client_id);

                if let Err(err) = sx.lock().await.close().await {
                    warn!("error closing connection for {}: {err}", &client_id);
                }

                break;
            }
        }
    }

    if let Some(client) = state.clients.lock().await.remove(&cid) {
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

    if let Some(mut client) = state.clients.lock().await.remove(&cid) {
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

    TOTAL_CLIENTS.set(state.clients.lock().await.len() as f64);

    debug!("total connected clients: {}", TOTAL_CLIENTS.get());
}
