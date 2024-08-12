use axum::extract::ws::{Message, WebSocket};

use anyhow::bail;
use futures::stream::SplitSink;
use futures::SinkExt;
use rhiaqey_sdk_rs::channel::Channel;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct WebSocketClient {
    pub hub_id: String,
    pub client_id: String,
    pub user_id: Option<String>,
    pub channels: Vec<(Channel, Option<String>, Option<String>)>,
    sender: Arc<Mutex<SplitSink<WebSocket, Message>>>,
}

impl WebSocketClient {
    pub fn create(
        hub_id: String,
        client_id: String,
        user_id: Option<String>,
        channels: Vec<(Channel, Option<String>, Option<String>)>,
        sender: Arc<Mutex<SplitSink<WebSocket, Message>>>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            hub_id,
            client_id,
            user_id,
            channels,
            sender,
        })
    }

    pub async fn send(&mut self, message: Message) -> anyhow::Result<()> {
        match self.sender.lock().await.send(message).await {
            Ok(_) => Ok(()),
            Err(err) => bail!(err.to_string()),
        }
    }
}
