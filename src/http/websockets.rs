use crate::http::state::SharedState;
use crate::hub::client::WebSocketClient;
use crate::hub::metrics::TOTAL_CLIENTS;

use crate::hub::channels::StreamingChannel;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Query, State, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum_client_ip::InsecureClientIp;
use axum_extra::{headers, TypedHeader};
use futures::stream::{SelectAll, SplitSink};
use futures::{SinkExt, StreamExt};
use log::{debug, info, trace, warn};
use rhiaqey_common::client::{
    ClientMessage, ClientMessageDataType, ClientMessageValue,
    ClientMessageValueClientChannelSubscription, ClientMessageValueClientConnection,
};
use rhiaqey_sdk_rs::channel::Channel;
use rusty_ulid::generate_ulid_string;
use serde::Deserialize;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

#[derive(Deserialize)]
pub struct Params {
    channels: String,
    snapshot: Option<bool>,
}

/// The handler for the HTTP request (this gets called when the HTTP GET lands at the start
/// of websocket negotiation). After this completes, the actual switching from HTTP to
/// websocket protocol will occur.
/// This is the last point where we can extract TCP/IP metadata such as IP address of the client
/// as well as things from HTTP headers such as user-agent of the browser etc.
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
    let mut streaming_channels = state.streams.write().unwrap();

    let (sender, receiver) = socket.split();
    let sx = Arc::new(Mutex::new(sender));
    let rx = Arc::new(Mutex::new(receiver));

    streaming_channels.iter_mut().for_each(|x| {
        if channels.contains(&x.get_name()) {
            x.add_client(client_id.clone(), sx.clone());
        }
    });

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

    if let Ok(raw) = serde_json::to_vec(&client_message) {
        if let Err(e) = sx.lock().await.send(Message::Binary(raw)).await {
            warn!("Could not send binary data due to {}", e);
        }
    }

    streaming_channels.iter_mut().for_each(|x| {
        if channels.contains(&x.get_name()) {
            let channel_name = x.get_name();
            let mut data = client_message.clone();

            data.data_type = ClientMessageDataType::ClientChannelSubscription as u8;
            data.channel = channel_name.clone().into();
            data.key = channel_name.clone().into();
            data.value = ClientMessageValue::ClientChannelSubscription(
                ClientMessageValueClientChannelSubscription {
                    channel: x.channel.clone(),
                },
            );

            if let Ok(raw) = serde_json::to_vec(&data) {
                x.notify_one(client_id.clone(), raw);
            }

            if snapshot_request {
                tokio::spawn(handle_snapshot(
                    channel_name.to_string(),
                    hub_id.clone(),
                    client_id.clone(),
                    sx.clone(),
                    state.streams.clone(),
                ));
            }
        }
    });

    // TODO: listen here for messages and if clients send anything disconnect them

    // for channel in channels {
    /*
    let streaming_channel = streaming_channels.get_mut(channel.as_str());
    if let Some(chx) = streaming_channel {
        chx.add_client(client_id.clone()).await;
        added_channels.push(chx.channel.clone());
        debug!("client joined channel {}", channel.as_str());
    } else {
        warn!("could not find channel {}", channel.as_str());
    }*/
    // }

    /*
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

        if let Err(e) = sx.lock().await.send(Message::Binary(raw)).await {
            warn!("Could not send binary data due to {}", e);
        }

        for channel in added_channels {
            let channel_name = channel.name.clone();
            let mut data = client_message.clone();

            data.data_type = ClientMessageDataType::ClientChannelSubscription as u8;
            data.channel = channel.name.clone().into();
            data.key = channel.name.clone().into();
            data.value = ClientMessageValue::ClientChannelSubscription(
                ClientMessageValueClientChannelSubscription { channel },
            );

            if let Ok(raw) = serde_json::to_vec(&data) {
                if let Ok(_) = sx.lock().await.send(Message::Binary(raw)).await {
                    trace!("channel subscription message sent successfully");
                } else {
                    warn!("could not send subscription message");
                }
            }

            if snapshot_request {
                tokio::spawn(handle_snapshot(
                    channel_name.to_string(),
                    hub_id.clone(),
                    client_id.clone(),
                    sx.clone(),
                    state.streams.clone(),
                ));
            }
        }

        let mut client = WebSocketClient::create(client_id.to_string(), sx, rx);

        client.listen();

        state.clients.lock().await.insert(client.get_id(), client);
        TOTAL_CLIENTS.set(state.clients.lock().await.len() as f64);
    */
    debug!("client {} was connected", client_id.clone());
}

async fn handle_snapshot(
    channel: String,
    hub_id: String,
    client_id: String,
    sx: Arc<Mutex<SplitSink<WebSocket, Message>>>,
    streams: Arc<RwLock<SelectAll<StreamingChannel>>>,
) {
    let mut streams_lock = streams.read().unwrap();
    let chx = streams_lock
        .iter_mut()
        .find(|x| x.get_name().eq(&channel))
        .unwrap();

    let messages: Vec<Vec<u8>> = chx
        .get_snapshot()
        .unwrap_or(vec![])
        .iter()
        .map(|x| ClientMessage::from(x))
        .map(|mut x| {
            if x.hub_id.is_none() {
                x.hub_id = Some(hub_id.clone());
            }

            return x;
        })
        .flat_map(|x| serde_json::to_vec(&x).ok())
        .collect();

    trace!(
        "sending {} channel[name={}] snapshot messages",
        messages.len(),
        chx.get_name()
    );

    for raw in messages {
        if let Ok(_) = sx.lock().await.send(Message::Binary(raw)).await {
            trace!(
                "channel[name={}] snapshot message sent successfully to {}",
                chx.get_name(),
                client_id
            );
        } else {
            warn!("could not send snapshot message to {client_id}");
        }
    }
}
