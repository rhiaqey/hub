use std::sync::Arc;
use std::{thread, time};

use log::{debug, warn};
use rhiaqey_common::redis::{self, RedisSettings};
use rhiaqey_common::topics;
use rhiaqey_sdk::channel::Channel;
use rustis::commands::{StreamCommands, StreamEntry, XReadGroupOptions};
use rustis::{
    client::Client,
    commands::{ConnectionCommands, PingOptions},
};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    Mutex, RwLock,
};

pub struct StreamingChannel {
    pub channel: Channel,
    pub namespace: String,
    pub sender: Option<UnboundedSender<u128>>,
    pub redis: Option<Arc<Mutex<Client>>>,
}

pub type HubMessageReceiver = Option<UnboundedReceiver<u128>>;

impl StreamingChannel {
    pub async fn create(namespace: String, channel: Channel) -> StreamingChannel {
        StreamingChannel {
            channel,
            namespace,
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
        self.redis = Some(Arc::new(Mutex::new(connection)));
    }

    pub async fn start(&mut self) {
        let one_sec = time::Duration::from_secs(1);
        let now = time::Instant::now();

        let sender = self.sender.clone().as_mut().unwrap().clone();

        let size = self.channel.size;
        let topic = topics::publishers_to_hub_stream_topic(
            self.namespace.clone(),
            self.channel.name.clone(),
        );

        let redis = self.redis.as_mut().unwrap().clone();

        tokio::task::spawn(async move {
            loop {
                let results: Vec<(String, Vec<StreamEntry<String>>)> = redis
                    .lock()
                    .await
                    .xreadgroup(
                        "hub",
                        "hub1",
                        XReadGroupOptions::default().count(size),
                        topic.clone(),
                        ">",
                    )
                    .await
                    .unwrap();
                if let Some(v) = results.get(0) {
                    debug!("results are in {:?}", v.0);
                }

                thread::sleep(one_sec);
            }
        });
    }

    pub fn get_name(&self) -> String {
        return self.channel.name.to_string();
    }
}
