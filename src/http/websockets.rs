use crate::http::state::SharedState;
use crate::hub::client::WebSocketClient;
use crate::hub::metrics::TOTAL_CLIENTS;

use crate::hub::simple_channel::SimpleChannels;
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
use rusty_ulid::generate_ulid_string;
use serde::Deserialize;
use std::sync::Arc;

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

/// Handle each client here
async fn handle_ws_client(
    socket: WebSocket,
    user_id: Option<String>,
    channels: SimpleChannels,
    snapshot_request: SnapshotParam,
    snapshot_size: Option<usize>,
    state: Arc<SharedState>,
) {
    let hub_id = state.get_id();
    let client_id = generate_ulid_string();

    info!("handle client {}", &client_id);
    debug!("{} channels extracted", channels.len());

    // With this, we will support channel names that can include categories seperated with a `/`
    // Valid examples would be `ticks` but also `ticks/historical`.
    // Any other format would be considered invalid and would be filtered out.
    let channels: Vec<(String, Option<String>, Option<String>)> =
        channels.get_channels_with_category_and_key();

    let mut added_channels: Vec<(Channel, Option<String>, Option<String>)> = vec![];

    {
        for channel in channels.iter() {
            let mut lock = state.streams.lock().await;
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

    let event_topic = topics::events_pubsub_topic(state.get_namespace());

    let client_message = ClientMessage {
        data_type: ClientMessageDataType::ClientConnection as u8,
        channel: String::from(""),
        key: String::from(""),
        value: ClientMessageValue::ClientConnection(ClientMessageValueClientConnection {
            client_id: client_id.clone(),
            hub_id: hub_id.to_string(),
        }),
        tag: None,
        category: None,
        hub_id: Some(hub_id.clone()),
        publisher_id: None,
    };

    let raw = serde_json::to_vec(&client_message).unwrap();

    let (mut sender, receiver) = socket.split();

    if let Err(e) = sender.send(Message::Binary(raw)).await {
        warn!("Could not send binary data due to {}", e);
    }

    sender.flush().await.expect("failed to flush messages");

    {
        for channel in added_channels.iter() {
            let channel_name = channel.0.name.clone();
            let mut data = client_message.clone();
            trace!("iterating channel {}", channel_name);

            data.data_type = ClientMessageDataType::ClientChannelSubscription as u8;
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

            let Ok(raw) = serde_json::to_vec(&data) else {
                warn!("failed to serialize to vec");
                continue;
            };

            if let Ok(_) = sender.send(Message::Binary(raw)).await {
                trace!("channel subscription message sent successfully");
            } else {
                warn!("could not send subscription message");
            }

            debug!("sending snapshot to client");
            let mut lock = state.streams.lock().await;
            let streaming_channel = lock.get_mut(&*channel_name);

            if let Some(chx) = streaming_channel {
                if snapshot_request.allowed() {
                    let snapshot = chx
                        .get_snapshot(
                            &snapshot_request,
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
                            client_message.hub_id = Some(hub_id.clone());
                        }

                        let raw = serde_json::to_vec(&client_message).unwrap();
                        if let Ok(_) = sender.send(Message::Binary(raw)).await {
                            trace!(
                                "channel snapshot message[category={:?}] sent successfully to {}",
                                channel.1,
                                &client_id
                            );
                        } else {
                            warn!("could not send snapshot message to {}", &client_id);
                            break;
                        }
                    }
                } else {
                    if let Some(raw) = chx.get_last_client_message(channel.1.clone()) {
                        trace!("sending last channel message instead");
                        if let Ok(_) = sender.send(Message::Binary(raw)).await {
                            trace!(
                                "last channel message[category={:?}] sent successfully to {}",
                                channel.1,
                                &client_id
                            );
                        } else {
                            warn!("could not send last channel message to {}", &client_id);
                            break;
                        }
                    }
                }
            }

            drop(lock);
        }
    }

    let cid = client_id.clone();
    let sx = Arc::new(tokio::sync::Mutex::new(sender));
    let rx = Arc::new(tokio::sync::Mutex::new(receiver));
    let client = WebSocketClient::create(
        cid.clone(),
        user_id.clone(),
        sx.clone(),
        added_channels.clone(),
    );

    if let Ok(raw) = serde_json::to_vec(&RPCMessage {
        data: RPCMessageData::ClientConnected(ClientConnectedMessage {
            client_id: cid.clone(),
            user_id: user_id.clone(),
            channels: added_channels.clone(),
        }),
    }) {
        let _: () = state
            .redis_rs
            .lock()
            .unwrap()
            .publish(&event_topic, raw)
            .expect("failed to publish message");
        debug!("event sent for client connect to {}", &event_topic);
    }

    client.flush().await.expect("failed to flush messages");

    state.clients.lock().await.insert(client_id.clone(), client);
    TOTAL_CLIENTS.set(state.clients.lock().await.len() as f64);

    debug!("client {client_id} was connected");
    debug!("total connected clients: {}", TOTAL_CLIENTS.get());

    let handler = tokio::spawn(async move {
        let client_id = client_id.clone();
        while let Some(Ok(msg)) = rx.lock().await.next().await {
            match msg {
                Message::Ping(_) => {}
                Message::Pong(_) => {}
                Message::Close(e) => {
                    let id = client_id.clone();

                    trace!("close message received for {id}");
                    if e.is_some() {
                        trace!("close frame also received for {id}");
                    }

                    if let Err(err) = sx.lock().await.close().await {
                        warn!("error closing connection for {id}: {err}");
                    }
                }
                Message::Text(data) => {
                    let id = client_id.clone();

                    match serde_json::from_str::<ClientMessage>(data.as_str()) {
                        Ok(e) => {
                            trace!("binary client message received for {id}: {:?}", e);
                            if e.data_type == ClientMessageDataType::Ping as u8 {
                                trace!("we have received a ping request from {id}");
                                continue;
                            }
                        }
                        Err(e) => {
                            trace!("corrupt text client message received: {e}");
                        }
                    }

                    if let Err(err) = sx.lock().await.close().await {
                        warn!("error closing connection for {id}: {err}");
                    }
                }
                Message::Binary(data) => {
                    let id = client_id.clone();

                    match serde_json::from_slice::<ClientMessage>(data.as_slice()) {
                        Ok(e) => {
                            trace!("binary client message received for {id}: {:?}", e);
                            if e.data_type == ClientMessageDataType::Ping as u8 {
                                trace!("we have received a ping request from {id}");
                                continue;
                            }
                        }
                        Err(e) => {
                            trace!("corrupt binary client message received: {e}");
                        }
                    }

                    if let Err(err) = sx.lock().await.close().await {
                        warn!("error closing connection for {id}: {err}");
                    }
                }
            }
        }
    });

    handler.await.expect("failed to listen client");

    if let Some(client) = state.clients.lock().await.remove(&cid) {
        trace!("client was removed from all clients");
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

    TOTAL_CLIENTS.set(state.clients.lock().await.len() as f64);

    if let Ok(raw) = serde_json::to_vec(&RPCMessage {
        data: RPCMessageData::ClientDisconnected(ClientDisconnectedMessage {
            client_id: cid.clone(),
            user_id: user_id.clone(),
            channels: added_channels.clone(),
        }),
    }) {
        let _: () = state
            .redis_rs
            .lock()
            .unwrap()
            .publish(&event_topic, raw)
            .expect("failed to publish message");
        debug!("event sent for client disconnect to {}", &event_topic);
    }

    debug!("client {cid} was disconnected");
    debug!("total connected clients: {}", TOTAL_CLIENTS.get());
}
