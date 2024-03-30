use axum::extract::ws::{Message, WebSocket};

use axum::Error;
use futures::stream::SplitSink;
use futures::SinkExt;
use rhiaqey_sdk_rs::channel::Channel;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct WebSocketClient {
    pub id: String,
    pub sender: Arc<Mutex<SplitSink<WebSocket, Message>>>,
    pub channels: Vec<(Channel, Option<String>)>,
}

impl WebSocketClient {
    pub fn create(
        client_id: String,
        sender: Arc<Mutex<SplitSink<WebSocket, Message>>>,
        channels: Vec<(Channel, Option<String>)>,
    ) -> WebSocketClient {
        WebSocketClient {
            id: client_id,
            sender,
            channels,
        }
    }

    pub async fn send(&mut self, message: Message) -> Result<(), Error> {
        self.sender.lock().await.send(message).await
    }
}
