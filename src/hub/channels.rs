use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::hub::messages::MessageHandler;
use log::warn;
use rhiaqey_common::redis::connect_and_ping;
use rhiaqey_common::redis::RedisSettings;
use rhiaqey_common::stream::StreamMessage;
use rhiaqey_common::topics;
use rhiaqey_sdk::channel::Channel;
use rustis::client::Client;
use rustis::commands::{StreamCommands, StreamEntry, XReadGroupOptions};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

pub struct StreamingChannel {
    pub channel: Channel,
    pub namespace: String,
    pub redis: Option<Arc<Mutex<Client>>>,
    pub message_handler: Option<Arc<Mutex<MessageHandler>>>,
    join_handler: Option<Arc<JoinHandle<u32>>>,
    pub clients: Arc<Mutex<Vec<Uuid>>>,
}

impl StreamingChannel {
    pub async fn create(namespace: String, channel: Channel) -> StreamingChannel {
        StreamingChannel {
            channel,
            namespace,
            redis: None,
            message_handler: None,
            join_handler: None,
            clients: Arc::new(Mutex::new(vec![])),
        }
    }

    pub async fn setup(&mut self, config: RedisSettings) {
        let connection = connect_and_ping(config.clone()).await.unwrap();
        self.redis = Some(Arc::new(Mutex::new(connection)));
        self.message_handler = Some(Arc::new(Mutex::new(
            MessageHandler::create(self.namespace.clone(), self.channel.clone(), config).await,
        )));
    }

    pub async fn start(&mut self) {
        let duration = Duration::from_millis(250);

        let size = self.channel.size;
        let topic = topics::publishers_to_hub_stream_topic(
            self.namespace.clone(),
            self.channel.name.clone(),
        );

        let redis = self.redis.as_mut().unwrap().clone();
        let message_handler = self.message_handler.as_mut().unwrap().clone();

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

                if let Err(e) = results {
                    warn!("error with retrieving results: {}", e);
                    thread::sleep(Duration::from_secs(1));
                    continue;
                }

                if let Some(v) = results.unwrap().get(0) {
                    if let Some(raw) = v.1.get(0).unwrap().items.get("raw") {
                        let stream_message: StreamMessage =
                            serde_json::from_str(raw.as_str()).unwrap();
                        message_handler
                            .lock()
                            .await
                            .handle_raw_stream_message(stream_message)
                            .await;
                    }
                }

                thread::sleep(duration);
            }
        });

        self.join_handler = Some(Arc::new(join_handler));
    }

    pub fn get_name(&self) -> String {
        return self.channel.name.to_string();
    }

    pub async fn add_client(&mut self, connection_id: Uuid) {
        self.clients.lock().await.push(connection_id);
    }
}

impl Drop for StreamingChannel {
    fn drop(&mut self) {
        self.join_handler.as_mut().unwrap().abort();
    }
}
