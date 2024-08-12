use anyhow::bail;
use rhiaqey_sdk_rs::channel::Channel;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct SSEClient {
    pub hub_id: String,
    pub client_id: String,
    pub user_id: Option<String>,
    pub channels: Vec<(Channel, Option<String>, Option<String>)>,
    sender: Arc<Mutex<tokio::sync::broadcast::Sender<String>>>,
}

impl SSEClient {
    pub fn create(
        hub_id: String,
        client_id: String,
        user_id: Option<String>,
        channels: Vec<(Channel, Option<String>, Option<String>)>,
        sender: Arc<Mutex<tokio::sync::broadcast::Sender<String>>>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            hub_id,
            client_id,
            user_id,
            channels,
            sender,
        })
    }

    pub async fn send(&mut self, message: String) -> anyhow::Result<()> {
        match self.sender.lock().await.send(message) {
            Ok(_) => Ok(()),
            Err(err) => bail!(err.to_string()),
        }
    }
}
