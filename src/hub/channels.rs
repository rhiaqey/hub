use std::sync::Arc;

use log::warn;
use rhiaqey_common::{stream::StreamMessage, redis::{RedisSettings, self}};
use rhiaqey_sdk::channel::Channel;
use rustis::{client::Client, commands::{PingOptions, ConnectionCommands}};
use tokio::sync::{mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender}, RwLock};

pub struct StreamingChannel {
    pub channel: Channel,
    pub sender: UnboundedSender<StreamMessage>,
    pub receiver: UnboundedReceiver<StreamMessage>,
    redis: Arc<RwLock<Client>>,
}

impl StreamingChannel {
    pub async fn create(channel: Channel, config: RedisSettings) -> Option<StreamingChannel> {
        let (tx, rx) = unbounded_channel::<StreamMessage>();
        let connection = Self::redis_connect(config).await?;
        Some(StreamingChannel {
            channel,
            sender: tx,
            receiver: rx,
            redis: Arc::new(RwLock::new(connection)),
        })
    }

    async fn redis_connect(config: RedisSettings) -> Option<Client> {
        let redis_connection = redis::connect(config).await;
        if redis_connection.is_none() {
            warn!("could not connect to redis server");
            return None;
        }

        let result: String = redis_connection
            .clone()
            .unwrap()
            .ping(PingOptions::default().message("hello"))
            .await
            .unwrap();
        if result != "hello" {
            warn!("redis ping failed");
            return None;
        }

        redis_connection
    }

    pub fn start(&self) {
        tokio::spawn(async move {
            //
        });
    }

    pub fn get_name(&self) -> String {
        return self.channel.name.to_string();
    }
}
