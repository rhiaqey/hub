use crate::http::client::WebSocketClient;
use crate::hub::channels::StreamingChannel;
use crate::hub::settings::HubSettings;
use rhiaqey_common::env::Env;
use rhiaqey_sdk_rs::channel::ChannelList;
use rhiaqey_sdk_rs::message::MessageValue;
use rustis::client::Client;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

pub struct SharedState {
    pub env: Arc<Env>,
    pub settings: Arc<RwLock<HubSettings>>,
    pub redis: Arc<Mutex<Option<Client>>>,
    pub streams: Arc<Mutex<HashMap<String, StreamingChannel>>>,
    pub clients: Arc<Mutex<HashMap<Cow<'static, str>, WebSocketClient>>>,
}

impl SharedState {
    pub fn get_namespace(&self) -> String {
        return self.env.namespace.clone();
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct CreateChannelsRequest {
    #[serde(flatten)]
    pub channels: ChannelList,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct AssignChannelsRequest {
    pub name: String,
    pub channels: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteChannelsRequest {
    pub channels: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct UpdateSettingsRequest {
    pub name: String,
    pub settings: MessageValue,
}
