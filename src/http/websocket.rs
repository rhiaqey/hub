use crate::http::common::{
    get_channel_snapshot_for_client, notify_system_for_client_connect,
    notify_system_for_client_disconnect, prepare_channels,
    prepare_client_channel_subscription_messages, prepare_client_connection_message,
};
use crate::http::state::SharedState;
use crate::hub::client::HubClient;
use crate::hub::client::websocket::WebSocketClient;
use crate::hub::metrics::TOTAL_CLIENTS;
use crate::hub::simple_channel::SimpleChannels;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{Query, State, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum_client_ip::ClientIp;
use axum_extra::{TypedHeader, headers};
use futures::{SinkExt, StreamExt};
use log::{debug, info, trace, warn};
use rusty_ulid::generate_ulid_string;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::common::{Params, SnapshotParam};

/// The handler for the HTTP request (this gets called when the HTTP GET lands at the start
/// of websocket negotiation). After this completes, the actual switching from HTTP to
/// websocket protocol will occur.
/// This is the last point where we can extract TCP/IP metadata such as IP address of the client
/// as well as things from HTTP headers such as user-agent of the browser, etc.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    // headers: HeaderMap,
    Query(params): Query<Params>,
    ClientIp(user_ip): ClientIp,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
    State(state): State<Arc<SharedState>>,
) -> impl IntoResponse {
    info!("[GET] Handle websocket connection");

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

    // finalize the upgrade process by returning upgrade callback.
    // we can customize the callback by sending additional info such as address.
    ws.on_upgrade(move |socket| {
        handle_ws_connection(
            socket,
            user_ip.to_string(),
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

/// Handle each websocket client here
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
    let mut client = HubClient::WebSocket(
        WebSocketClient::create(
            state.get_id().to_string(),
            client_id.clone(),
            user_id.clone(),
            sx.clone(),
            channels.clone(),
        )
        .unwrap(),
    );

    // 1. send client connect message

    match prepare_client_connection_message(client.get_client_id(), client.get_hub_id()) {
        Ok(message) => match message.ser_to_msgpack() {
            Ok(raw) => match client.send_raw(raw).await {
                Ok(_) => debug!("client connection message sent successfully"),
                Err(err) => warn!("failed to send client message: {}", err),
            },
            Err(err) => warn!("failed to serialize to msgpack: {}", err),
        },
        Err(err) => warn!("failed to prepare connection message: {}", err),
    }

    // 2. send channel subscription

    match prepare_client_channel_subscription_messages(client.get_hub_id(), &channels) {
        Ok(messages) => {
            for message in messages {
                match message.ser_to_msgpack() {
                    Ok(raw) => match client.send_raw(raw).await {
                        Ok(_) => debug!("client channel subscription message sent successfully"),
                        Err(err) => warn!("failed to send client message: {}", err),
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
            match client.send_message(client_message).await {
                Ok(_) => trace!("snapshot message send successfully"),
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

    let cid = client.get_client_id().clone();
    state.clients.lock().await.insert(client_id.clone(), client);
    debug!("client {client_id} was connected");

    {
        let total = state.clients.lock().await.len() as i64;
        TOTAL_CLIENTS.set(total);
        debug!("total connected clients: {}", total);
    }

    // 6. listen for messages

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

    // 7. disconnect client and remove

    debug!("removing client connection");

    if let Some(client) = state.clients.lock().await.remove(&cid) {
        trace!("client was removed from all hub clients");

        // 7.1 remove from channels

        for channel in client.get_channels().iter() {
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

        // 7.2 notify for client disconnect

        match notify_system_for_client_disconnect(
            client.get_client_id(),
            client.get_user_id(),
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

    {
        let total_clients = state.clients.lock().await.len() as i64;
        TOTAL_CLIENTS.set(total_clients);
        debug!("total connected clients: {}", total_clients);
    }
}
