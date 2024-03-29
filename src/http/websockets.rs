use crate::http::state::SharedState;
use crate::hub::client::WebSocketClient;
use crate::hub::metrics::TOTAL_CLIENTS;

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Query, State, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum_client_ip::InsecureClientIp;
use axum_extra::{headers, TypedHeader};
use futures::{SinkExt, StreamExt};
use log::{debug, info, trace, warn};
use rhiaqey_common::client::{
    ClientMessage, ClientMessageDataType, ClientMessageValue,
    ClientMessageValueClientChannelSubscription, ClientMessageValueClientConnection,
};
use rhiaqey_sdk_rs::channel::Channel;
use rusty_ulid::generate_ulid_string;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
pub struct Params {
    channels: String,
    snapshot: Option<bool>,
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

    let channels: Vec<String> = params.channels.split(",").map(|x| x.to_string()).collect();
    trace!("channel from params extracted {:?}", channels);

    let snapshot_request = params.snapshot.unwrap_or(true);
    trace!("snapshot request: {}", snapshot_request);

    // finalize the upgrade process by returning upgrade callback.
    // we can customize the callback by sending additional info such as address.
    ws.on_upgrade(move |socket| handle_ws_connection(socket, ip, channels, snapshot_request, state))
}

/// Handle each websocket connection here
async fn handle_ws_connection(
    socket: WebSocket,
    ip: String,
    channels: Vec<String>,
    snapshot_request: bool,
    state: Arc<SharedState>,
) {
    info!("connection {ip} established");
    tokio::task::spawn(
        async move { handle_client(socket, channels, snapshot_request, state).await },
    );
}

/// Handle each client here
async fn handle_client(
    socket: WebSocket,
    channels: Vec<String>,
    snapshot_request: bool,
    state: Arc<SharedState>,
) {
    let hub_id = state.get_id();
    let client_id = generate_ulid_string();

    info!("handle client {client_id}");
    debug!("channels found {:?}", channels);

    let mut added_channels: Vec<Channel> = vec![];
    let mut streaming_channels = state.streams.lock().await;

    for channel in channels.iter() {
        let streaming_channel = streaming_channels.get_mut(channel);
        if let Some(chx) = streaming_channel {
            chx.add_client(client_id.clone());
            added_channels.push(chx.channel.clone());
            debug!("client joined channel {}", channel);
        } else {
            warn!("could not find channel {}", channel);
        }
    }

    let client_message = ClientMessage {
        data_type: ClientMessageDataType::ClientConnection as u8,
        channel: "".into(),
        key: "".into(),
        value: ClientMessageValue::ClientConnection(ClientMessageValueClientConnection {
            client_id: client_id.to_string(),
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

    for channel in added_channels.iter() {
        let channel_name = channel.name.clone();
        let mut data = client_message.clone();

        data.data_type = ClientMessageDataType::ClientChannelSubscription as u8;
        data.channel = channel.name.clone().into();
        data.key = channel.name.clone().into();
        data.value = ClientMessageValue::ClientChannelSubscription(
            ClientMessageValueClientChannelSubscription {
                channel: Channel {
                    name: channel.name.clone(),
                    size: channel.size,
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

        if snapshot_request {
            debug!("sending snapshot to client");
            let streaming_channel = streaming_channels.get_mut(&*channel_name);
            if let Some(chx) = streaming_channel {
                let snapshot = chx.get_snapshot().unwrap_or(vec![]);
                for stream_message in snapshot.iter() {
                    let mut client_message = ClientMessage::from(stream_message);
                    if client_message.hub_id.is_none() {
                        client_message.hub_id = Some(hub_id.clone());
                    }

                    let raw = serde_json::to_vec(&client_message).unwrap();
                    if let Ok(_) = sender.send(Message::Binary(raw)).await {
                        trace!("channel snapshot message sent successfully to {client_id}");
                    } else {
                        warn!("could not send snapshot message to {client_id}");
                        break;
                    }
                }
            }
        }
    }

    let cid = client_id.clone();
    let sx = Arc::new(tokio::sync::Mutex::new(sender));
    let rx = Arc::new(tokio::sync::Mutex::new(receiver));
    let client = WebSocketClient::create(client_id.clone(), sx.clone(), added_channels.clone());

    state.clients.lock().await.insert(client_id.clone(), client);
    TOTAL_CLIENTS.set(state.clients.lock().await.len() as f64);

    debug!("client {client_id} was connected");
    // NOTE: Required. Do not remove
    drop(streaming_channels);

    let handler = tokio::spawn(async move {
        let client_id = client_id.clone();
        while let Some(Ok(msg)) = rx.lock().await.next().await {
            match msg {
                Message::Ping(_) => {}
                Message::Pong(_) => {}
                Message::Close(_) | Message::Text(_) | Message::Binary(_) => {
                    let id = client_id.clone();
                    warn!("must close connection for client {id}");
                    if let Err(err) = sx.lock().await.close().await {
                        warn!("error closing connection for {id}: {err}");
                    }

                    break;
                }
            }
        }
    });

    handler.await.expect("failed to listen client");

    if let Some(client) = state.clients.lock().await.remove(&cid) {
        trace!("client was removed from all clients");
        for channel in client.channels.iter() {
            trace!("removing client from {} channel as well", channel.name);
            if let Some(sc) = state
                .streams
                .lock()
                .await
                .get_mut(&channel.name.to_string())
            {
                sc.remove_client(cid.clone());
            }
        }
    }
    TOTAL_CLIENTS.set(state.clients.lock().await.len() as f64);

    debug!("client {cid} was disconnected");
}
