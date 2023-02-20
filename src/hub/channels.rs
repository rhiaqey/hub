use std::sync::Arc;
use std::{thread, time};

use log::{info, warn};
use rhiaqey_common::redis::{self, RedisSettings};
use rhiaqey_common::stream::StreamMessage;
use rhiaqey_common::topics;
use rhiaqey_sdk::channel::Channel;
use rustis::commands::{StreamCommands, StreamEntry, XReadGroupOptions};
use rustis::{
    client::Client,
    commands::{ConnectionCommands, PingOptions},
};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    Mutex,
};
use tokio::task::JoinHandle;

pub struct StreamingChannel {
    pub channel: Channel,
    pub namespace: String,
    pub sender: Option<UnboundedSender<u128>>,
    pub redis: Option<Arc<Mutex<Client>>>,
    join_handler: Option<Arc<JoinHandle<u32>>>,
}

pub type HubMessageReceiver = Option<UnboundedReceiver<u128>>;

impl StreamingChannel {
    pub async fn create(namespace: String, channel: Channel) -> StreamingChannel {
        StreamingChannel {
            channel,
            namespace,
            sender: None,
            redis: None,
            join_handler: None,
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
        let one_sec = time::Duration::from_millis(500);

        let size = self.channel.size;
        let topic = topics::publishers_to_hub_stream_topic(
            self.namespace.clone(),
            self.channel.name.clone(),
        );

        let redis = self.redis.as_mut().unwrap().clone();

        let join_handler = tokio::task::spawn(async move {
            loop {
                let results: rustis::Result<Vec<(String, Vec<StreamEntry<String>>)>> = redis
                    .lock()
                    .await
                    .xreadgroup(
                        "hub",
                        "hub1",
                        XReadGroupOptions::default().count(size),
                        topic.clone(),
                        ">",
                    )
                    .await;

                if results.is_err() {
                    warn!("error with retrieving results");
                    continue;
                }

                if let Some(v) = results.unwrap().get(0) {
                    if let Some(raw) = v.1.get(0).unwrap().items.get("raw") {
                        let stream_message: StreamMessage =
                            serde_json::from_str(raw.as_str()).unwrap();
                        info!("channel list extracted {:?}", stream_message);
                    }
                }

                thread::sleep(one_sec);
            }
        });

        self.join_handler = Some(Arc::new(join_handler));
    }

    pub fn get_name(&self) -> String {
        return self.channel.name.to_string();
    }
}

impl Drop for StreamingChannel {
    fn drop(&mut self) {
        self.join_handler.as_mut().unwrap().abort();
    }
}
