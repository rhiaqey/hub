use log::{debug, warn};
use redis::Commands;
use rhiaqey_common::client::{ClientMessage, ClientMessageDataType, ClientMessageValue, ClientMessageValueClientChannelSubscription, ClientMessageValueClientConnection};
use rhiaqey_common::pubsub::{ClientConnectedMessage, ClientDisconnectedMessage, RPCMessage, RPCMessageData};
use rhiaqey_common::topics;
use rhiaqey_sdk_rs::channel::Channel;
use rhiaqey_sdk_rs::message::MessageValue;
use serde::Deserialize;

use crate::hub::{simple_channel::SimpleChannels, streaming_channel::StreamingChannel};
use std::collections::HashMap;
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
    pub fn allowed(&self) -> bool {
        match *self {
            SnapshotParam::ASC => true,
            SnapshotParam::DESC => true,
            SnapshotParam::TRUE => true,
            SnapshotParam::FALSE => false,
        }
    }
}

#[inline(always)]
pub async fn prepare_channels(
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

#[inline(always)]
pub fn prepare_client_channel_subscription_messages(
    hub_id: &String,
    channels: &Vec<(Channel, Option<String>, Option<String>)>,
) -> anyhow::Result<Vec<ClientMessage>> {
    let mut result = vec![];

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

        result.push(client_message)
    }

    Ok(result)
}

pub enum ChannelSnapshotResult {
    Messages(Vec<ClientMessage>),
    LastMessage(ClientMessage),
}

#[inline(always)]
pub async fn get_channel_snapshot_for_client(
    hub_id: &String,
    channels: &Vec<(Channel, Option<String>, Option<String>)>,
    streams: Arc<Mutex<HashMap<String, StreamingChannel>>>,
    snapshot_request: &SnapshotParam,
    snapshot_size: Option<usize>,
) -> HashMap<String, ChannelSnapshotResult> {
    let mut lock = streams.lock().await;
    let mut messages: HashMap<String, ChannelSnapshotResult> = HashMap::new();

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
                            client_message.hub_id = Some(hub_id.to_string());
                        }
                    } else {
                        client_message.hub_id = None;
                        client_message.publisher_id = None;
                    }

                    all.push(ClientMessage::from(stream_message));
                }

                messages.insert(channel_name, ChannelSnapshotResult::Messages(all));
            } else {
                match chx.get_last_client_message(channel.1.clone()) {
                    Some(message) => {
                        messages.insert(channel_name, ChannelSnapshotResult::LastMessage(message));
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

#[inline(always)]
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

#[inline(always)]
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
