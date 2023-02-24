use crate::http::state::SharedState;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{ConnectInfo, Query, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::{headers, TypedHeader};
use log::{debug, info, warn};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;
use uuid::Uuid;

//allows to split the websocket stream into separate TX and RX branches
use futures::{sink::SinkExt, stream::StreamExt};
use rhiaqey_common::client::{
    ClientMessage, ClientMessageDataType, ClientMessageValue,
    ClientMessageValueClientChannelSubscription, ClientMessageValueClientConnection,
};
use rhiaqey_sdk::channel::Channel;

#[derive(Deserialize)]
pub struct Params {
    channels: String,
}

/// The handler for the HTTP request (this gets called when the HTTP GET lands at the start
/// of websocket negotiation). After this completes, the actual switching from HTTP to
/// websocket protocol will occur.
/// This is the last point where we can extract TCP/IP metadata such as IP address of the client
/// as well as things from HTTP headers such as user-agent of the browser etc.
pub async fn ws_handler(
    Query(params): Query<Params>,
    ws: WebSocketUpgrade,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    state: Arc<SharedState>,
) -> impl IntoResponse {
    info!("[GET] Handle websocket connection");

    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
        user_agent.to_string()
    } else {
        String::from("Unknown browser")
    };

    debug!("`{}` at {} connected.", user_agent, addr.to_string());
    debug!("params extracted {:?}", params.channels);

    // finalize the upgrade process by returning upgrade callback.
    // we can customize the callback by sending additional info such as address.
    ws.on_upgrade(move |socket| {
        handle_socket(
            socket,
            addr,
            params.channels.split(",").map(|x| x.to_string()).collect(),
            state,
        )
    })
}

/// Actual websocket state machine (one will be spawned per connection)
async fn handle_socket(
    socket: WebSocket,
    _who: SocketAddr,
    channels: Vec<String>,
    state: Arc<SharedState>,
) {
    debug!("channels found {:?}", channels);

    let client_id = Uuid::new_v4();
    let mut added_channels: Vec<Channel> = vec![];
    let mut streaming_channels = state.streams.lock().await;

    for channel in channels {
        let streaming_channel = streaming_channels.get_mut(channel.as_str());
        if let Some(chx) = streaming_channel {
            chx.join_client(client_id).await;
            added_channels.push(chx.channel.clone());
            debug!("client joined channel {}", channel.as_str());
        } else {
            warn!("could not find channel {}", channel.as_str());
        }
    }

    let (mut sender, mut _receiver) = socket.split();

    let client_message = ClientMessage {
        data_type: ClientMessageDataType::ClientConnection as u8,
        channel: "".to_string(),
        key: "".to_string(),
        value: ClientMessageValue::ClientConnection(ClientMessageValueClientConnection {
            client_id: client_id.to_string(),
        }),
        tag: None,
        category: None,
        hub_id: None,
        publisher_id: None,
    };

    let raw = serde_json::to_vec(&client_message).unwrap();

    if let Err(e) = sender.send(Message::Binary(raw)).await {
        warn!("Could not send binary data due to {}", e);
        return;
    }

    debug!("client just joined in");

    for channel in added_channels {
        let mut data = client_message.clone();

        data.data_type = ClientMessageDataType::ClientChannelSubscription as u8;
        data.channel = channel.name.clone();
        data.key = channel.name.clone();
        data.value = ClientMessageValue::ClientChannelSubscription(
            ClientMessageValueClientChannelSubscription {
                client_id: client_id.to_string(),
                channel,
            },
        );

        let raw = serde_json::to_vec(&data).unwrap();
        if let Err(e) = sender.send(Message::Binary(raw)).await {
            warn!("Could not send binary data due to {}", e);
        }
    }

    state.clients.lock().await.insert(client_id, sender);

    debug!("client was stored")
}
