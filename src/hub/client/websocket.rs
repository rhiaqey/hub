use axum::extract::ws::{Message, WebSocket};

use anyhow::bail;
use futures::stream::SplitSink;
use futures::SinkExt;
use rhiaqey_sdk_rs::channel::Channel;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct WebSocketClient {
    hub_id: String,
    client_id: String,
    user_id: Option<String>,
    sender: Arc<Mutex<SplitSink<WebSocket, Message>>>,
    pub channels: Vec<(Channel, Option<String>, Option<String>)>,
}

impl WebSocketClient {
    pub fn create(
        hub_id: String,
        client_id: String,
        user_id: Option<String>,
        sender: Arc<Mutex<SplitSink<WebSocket, Message>>>,
        channels: Vec<(Channel, Option<String>, Option<String>)>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            hub_id,
            client_id,
            user_id,
            sender,
            channels,
        })
    }

    pub fn get_hub_id(&self) -> &String {
        &self.hub_id
    }

    pub fn get_client_id(&self) -> &String {
        &self.client_id
    }

    pub fn get_user_id(&self) -> &Option<String> {
        &self.user_id
    }

    pub async fn send(&mut self, message: Message) -> anyhow::Result<()> {
        match self.sender.lock().await.send(message).await {
            Ok(_) => Ok(()),
            Err(err) => bail!(err.to_string()),
        }
    }

    pub fn get_category_for_channel(
        &self,
        name: &String,
    ) -> Option<(&Option<String>, &Option<String>)> {
        self.channels.iter().find_map(|(channel, category, key)| {
            if channel.get_name().eq(name) {
                Some((category, key))
            } else {
                None
            }
        })
    }
}
