use log::{debug, warn};
use redis::Commands;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use rhiaqey_common::{
    client::{
        ClientMessage, ClientMessageDataType, ClientMessageValue,
        ClientMessageValueClientChannelSubscription, ClientMessageValueClientConnection,
    },
    pubsub::{ClientConnectedMessage, ClientDisconnectedMessage, RPCMessage, RPCMessageData},
    topics,
};
use rhiaqey_sdk_rs::{channel::Channel, message::MessageValue};
use serde::Deserialize;

use crate::hub::{
    client::HubClient, simple_channel::SimpleChannels, streaming_channel::StreamingChannel,
};

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
    pub fn allowed(&self) -> bool {
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
    pub channels: String,
    pub snapshot: Option<SnapshotParam>,
    pub snapshot_size: Option<usize>,
    pub user_id: Option<String>,
}

pub fn prepare_client_connection_message(
    client_id: &String,
    hub_id: &String,
) -> anyhow::Result<ClientMessage> {
    let mut client_message = ClientMessage {
        data_type: ClientMessageDataType::ClientConnection as u8,
        channel: String::from(""),
        key: String::from(""),
        value: ClientMessageValue::ClientConnection(ClientMessageValueClientConnection {
            client_id: client_id.to_string(),
            hub_id: hub_id.to_string(),
        }),
        tag: None,
        category: None,
        hub_id: None,
        publisher_id: None,
    };

    if cfg!(debug_assertions) {
        client_message.hub_id = Some(hub_id.clone());
    }

    Ok(client_message)
}

pub fn prepare_client_channel_subscription_messages(
    hub_id: &String,
    channels: &Vec<(Channel, Option<String>, Option<String>)>,
) -> anyhow::Result<Vec<ClientMessage>> {
    let mut client_message = ClientMessage {
        data_type: ClientMessageDataType::ClientChannelSubscription as u8,
        channel: "".to_string(),
        key: "".to_string(),
        value: ClientMessageValue::Data(MessageValue::Text(String::from(""))),
        tag: None,
        category: None,
        hub_id: None,
        publisher_id: None,
    };

    if cfg!(debug_assertions) {
        client_message.hub_id = Some(hub_id.clone());
    }

    let mut result: Vec<ClientMessage> = vec![];

    for channel in channels {
        let mut client_message = client_message.clone();
        client_message.channel = channel.0.name.to_string();
        client_message.key = channel.0.name.to_string();
        client_message.category = channel.1.clone();
        client_message.value = ClientMessageValue::ClientChannelSubscription(
            ClientMessageValueClientChannelSubscription {
                channel: Channel {
                    name: channel.0.name.clone(),
                    size: channel.0.size,
                },
            },
        );

        result.push(client_message);
    }

    Ok(result)
}

pub async fn prepare_channels(
    client_id: &String,
    channels: SimpleChannels,
    streams: Arc<Mutex<HashMap<String, StreamingChannel>>>,
) -> Vec<(Channel, Option<String>, Option<String>)> {
    // With this, we will support channel names that can include categories separated with a `/`
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

pub fn notify_system_for_client_connect(
    client_id: &String,
    user_id: &Option<String>,
    namespace: &str,
    channels: &Vec<(Channel, Option<String>, Option<String>)>,
    redis: Arc<std::sync::Mutex<redis::Connection>>,
) -> anyhow::Result<()> {
    let raw = serde_json::to_vec(&RPCMessage {
        data: RPCMessageData::ClientConnected(ClientConnectedMessage {
            client_id: client_id.clone(),
            user_id: user_id.clone(),
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

pub fn notify_system_for_client_disconnect(
    client_id: &String,
    user_id: &Option<String>,
    namespace: &str,
    channels: &Vec<(Channel, Option<String>, Option<String>)>,
    redis: Arc<std::sync::Mutex<redis::Connection>>,
) -> anyhow::Result<()> {
    let raw = serde_json::to_vec(&RPCMessage {
        data: RPCMessageData::ClientDisconnected(ClientDisconnectedMessage {
            client_id: client_id.clone(),
            user_id: user_id.clone(),
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

pub async fn retrieve_schema_for_hub(
    namespace: &str,
    redis: Arc<std::sync::Mutex<redis::Connection>>,
) -> anyhow::Result<serde_json::Value> {
    let schema_key: String = topics::hub_schema_key(namespace);

    let schema_str: String = redis.lock().unwrap().get(schema_key)?;
    let schema = serde_json::from_str(schema_str.as_str())?;

    Ok(schema)
}

pub async fn get_channel_snapshot_for_client(
    client: &HubClient,
    channels: &Vec<(Channel, Option<String>, Option<String>)>,
    streams: Arc<Mutex<HashMap<String, StreamingChannel>>>,
    snapshot_request: &SnapshotParam,
    snapshot_size: Option<usize>,
) -> HashMap<String, Vec<ClientMessage>> {
    let mut lock = streams.lock().await;
    let mut messages: HashMap<String, Vec<ClientMessage>> = HashMap::new();

    for channel in channels.iter() {
        let channel_name = channel.0.name.clone();
        let streaming_channel = lock.get_mut(&*channel_name);
        if let Some(chx) = streaming_channel {
            if snapshot_request.allowed() {
                let snapshot = chx
                    .get_snapshot(
                        snapshot_request,
                        channel.1.clone(),
                        channel.2.clone(),
                        snapshot_size,
                    )
                    .unwrap_or(vec![]);

                let mut all: Vec<ClientMessage> = vec![];

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
                            client_message.hub_id = Some(client.get_hub_id().clone());
                        }
                    } else {
                        client_message.hub_id = None;
                        client_message.publisher_id = None;
                    }

                    all.push(ClientMessage::from(stream_message));
                }

                messages.insert(channel_name, all);
            } else {
                match chx.get_last_client_message(channel.1.clone()) {
                    Some(message) => {
                        messages.insert(channel_name, vec![message]);
                    }
                    None => {
                        warn!("failed to fetch last client message from channel");
                        continue;
                    }
                }
            }
        }
    }

    messages
}
