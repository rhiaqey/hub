use crate::hub::websocket_client::WebSocketClient;
#[cfg(not(debug_assertions))]
use crate::hub::settings::HubSettings;
use crate::hub::streaming_channel::StreamingChannel;
use anyhow::Context;
use log::{debug, info, trace};
use redis::Commands;
use rhiaqey_common::env::Env;
use rhiaqey_common::pubsub::{RPCMessage, RPCMessageData};
use rhiaqey_common::security::SecurityKey;
use rhiaqey_common::{security, topics};
use rhiaqey_sdk_rs::channel::ChannelList;
use rhiaqey_sdk_rs::message::MessageValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

pub struct SharedState {
    pub env: Arc<Env>,
    pub redis_rs: Arc<std::sync::Mutex<redis::Connection>>,
    pub security: Arc<RwLock<SecurityKey>>,
    #[cfg(not(debug_assertions))]
    pub settings: Arc<RwLock<HubSettings>>,
    pub clients: Arc<Mutex<HashMap<String, WebSocketClient>>>,
    pub streams: Arc<Mutex<HashMap<String, StreamingChannel>>>,
}

impl SharedState {
    #[inline]
    pub fn get_id(&self) -> &str {
        self.env.get_id()
    }

    #[inline]
    pub fn get_name(&self) -> &str {
        self.env.get_name()
    }

    #[inline]
    pub fn get_namespace(&self) -> &str {
        self.env.get_namespace()
    }

    pub fn publish_rpc_message(&self, data: RPCMessageData) -> anyhow::Result<()> {
        info!("broadcasting to all hubs for message {}", data);

        let lock = self.redis_rs.clone();
        let mut conn = lock.lock().unwrap();

        let rpc_message = RPCMessage { data }
            .ser_to_string()
            .context("failed to serialize rpc message")?;

        let hub_broadcast_topic = topics::hub_raw_to_hub_clean_pubsub_topic(self.get_namespace());
        debug!("broadcasting to topic {}", hub_broadcast_topic);

        let _ = conn
            .publish(hub_broadcast_topic, rpc_message)
            .context("failed to publish message")?;

        info!("broadcast message sent");

        drop(conn);

        Ok(())
    }

    pub fn store_settings(&self, topic: String, data: Vec<u8>) -> anyhow::Result<()> {
        trace!("storing settings to {}", topic);

        let keys = self.security.read().unwrap();
        let settings = security::aes_encrypt(
            keys.no_once.as_slice(),
            keys.key.as_slice(),
            data.as_slice(),
        )?;

        trace!("settings encrypted");

        let lock = self.redis_rs.clone();
        let mut conn = lock.lock().unwrap(); // blocking

        trace!("redis connection acquired");

        let _ = conn
            .set(topic, settings)
            .context("failed to store settings for publisher")?;

        trace!("store setting successful");

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct CreateChannelsRequest {
    /// List of new channels
    #[serde(flatten)]
    pub channels: ChannelList,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct AssignChannelsRequest {
    /// Publisher's name
    pub name: String,
    /// List of registered channels
    pub channels: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteChannelsRequest {
    /// List of registered channels
    pub channels: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct UpdateSettingsRequest {
    /// Publisher's name
    pub name: String,
    /// Publisher's settings
    pub settings: MessageValue,
}
