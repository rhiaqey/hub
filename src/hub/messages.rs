use log::info;
use rhiaqey_common::redis::{connect_and_ping, RedisSettings};
use rhiaqey_common::stream::StreamMessage;
use rustis::client::Client;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct MessageHandler {
    pub redis: Option<Arc<Mutex<Client>>>,
}

impl MessageHandler {
    pub async fn create(config: RedisSettings) -> MessageHandler {
        let connection = connect_and_ping(config).await.unwrap();
        MessageHandler {
            redis: Some(Arc::new(Mutex::new(connection))),
        }
    }

    pub fn handle_raw_stream_message(&self, stream_message: StreamMessage) {
        info!("stream list handled {:?}", stream_message);
    }
}
