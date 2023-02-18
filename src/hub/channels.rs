use std::sync::Arc;
use std::{thread, time};

use log::warn;
use rhiaqey_common::redis::{self, RedisSettings};
use rhiaqey_sdk::channel::Channel;
use rustis::{
    client::Client,
    commands::{ConnectionCommands, PingOptions},
};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    RwLock,
};

pub struct StreamingChannel {
    pub channel: Channel,
    pub sender: Option<UnboundedSender<u128>>,
    pub redis: Option<Arc<RwLock<Client>>>,
}

pub type HubMessageReceiver = Option<UnboundedReceiver<u128>>;

impl StreamingChannel {
    pub async fn create(channel: Channel) -> StreamingChannel {
        StreamingChannel {
            channel,
            sender: None,
            redis: None,
        }
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

    pub async fn setup(&mut self, sender: UnboundedSender<u128>, config: RedisSettings) {
        let connection = Self::redis_connect(config).await.unwrap();
        self.sender = Some(sender);
        self.redis = Some(Arc::new(RwLock::new(connection)));
    }

    pub fn start(&mut self) {
        let ten_millis = time::Duration::from_secs(1);
        let now = time::Instant::now();

        let sender = self.sender.clone().as_mut().unwrap().clone();

        thread::spawn(move || loop {
            sender.send(now.elapsed().as_millis()).unwrap();
            thread::sleep(ten_millis);
        });
    }

    pub fn get_name(&self) -> String {
        return self.channel.name.to_string();
    }
}
