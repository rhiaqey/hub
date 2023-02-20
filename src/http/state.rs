use crate::hub::channels::StreamingChannel;
use rhiaqey_common::env::Env;
use rhiaqey_sdk::channel::ChannelList;
use rustis::client::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;

pub struct SharedState {
    pub env: Arc<Env>,
    pub redis: Arc<Mutex<Option<Client>>>,
    pub sender: UnboundedSender<u128>,
    pub streams: Arc<Mutex<HashMap<String, StreamingChannel>>>,
}

impl SharedState {
    pub fn get_namespace(&self) -> String {
        return self.env.namespace.clone();
    }
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct CreateChannelsRequest {
    #[serde(flatten)]
    pub channels: ChannelList,
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct AssignChannelsRequest {
    pub name: String,
    pub channels: Vec<String>,
}

#[derive(Serialize, Deserialize, Default, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteChannelsRequest {
    pub channels: Vec<String>,
}
