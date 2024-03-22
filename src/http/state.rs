use crate::hub::channel::StreamingChannel;
use crate::hub::client::WebSocketClient;
use crate::hub::settings::HubSettings;
use rhiaqey_common::env::Env;
use rhiaqey_common::security::SecurityKey;
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
    pub security: Arc<Mutex<SecurityKey>>,
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
