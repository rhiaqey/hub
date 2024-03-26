use crate::hub::channel::StreamingChannel;
use crate::hub::client::WebSocketClient;
use crate::hub::settings::HubSettings;
use log::{debug, info};
use redis::Commands;
use rhiaqey_common::env::Env;
use rhiaqey_common::pubsub::{RPCMessage, RPCMessageData};
use rhiaqey_common::security::SecurityKey;
use rhiaqey_common::{topics, RhiaqeyResult};
use rhiaqey_sdk_rs::channel::ChannelList;
use rhiaqey_sdk_rs::message::MessageValue;
use rustis::client::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

pub struct SharedState {
    pub env: Arc<Env>,
    pub redis: Arc<Mutex<Client>>,
    pub redis_rs: Arc<std::sync::Mutex<redis::Connection>>,
    pub security: Arc<RwLock<SecurityKey>>,
    pub settings: Arc<RwLock<HubSettings>>,
    pub clients: Arc<Mutex<HashMap<String, WebSocketClient>>>,
    pub streams: Arc<Mutex<HashMap<String, StreamingChannel>>>,
}

impl SharedState {
    pub fn get_id(&self) -> String {
        return self.env.id.to_string();
    }

    pub fn get_name(&self) -> String {
        return self.env.name.to_string();
    }

    pub fn get_namespace(&self) -> String {
        return self.env.namespace.to_string();
    }

    pub fn publish_rpc_message(&self, data: RPCMessageData) -> RhiaqeyResult<()> {
        info!("broadcasting to all hubs");

        let lock = self.redis_rs.clone();
        let mut conn = lock.lock().unwrap();

        let rpc_message = serde_json::to_string(&RPCMessage { data })
            .map_err(|x| x.to_string())?;

        let hub_broadcast_topic = topics::hub_raw_to_hub_clean_pubsub_topic(self.get_namespace());
        debug!("broadcasting to topic {}", hub_broadcast_topic);

        let _ = conn
            .publish(hub_broadcast_topic, rpc_message)
            .map_err(|x| x.to_string())?;

        info!("broadcast message sent");

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
