use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::hub::messages::MessageHandler;
use log::{debug, warn};
use rhiaqey_common::redis::connect_and_ping;
use rhiaqey_common::redis::RedisSettings;
use rhiaqey_common::stream::StreamMessage;
use rhiaqey_common::topics;
use rhiaqey_sdk::channel::Channel;
use rustis::client::Client;
use rustis::commands::{
    GenericCommands, ScanOptions, StreamCommands, StreamEntry, XReadGroupOptions, XReadOptions,
};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

pub struct StreamingChannel {
    hub_id: String,
    pub channel: Channel,
    pub namespace: String,
    pub redis: Option<Arc<Mutex<Client>>>,
    pub message_handler: Option<Arc<Mutex<MessageHandler>>>,
    join_handler: Option<Arc<JoinHandle<u32>>>,
    pub clients: Arc<Mutex<Vec<Uuid>>>,
}

impl StreamingChannel {
    pub async fn create(hub_id: String, namespace: String, channel: Channel) -> StreamingChannel {
        StreamingChannel {
            hub_id,
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
            MessageHandler::create(
                self.hub_id.clone(),
                self.namespace.clone(),
                self.channel.clone(),
                config,
            )
            .await,
        )));
    }

    pub async fn start(&mut self) {
        let duration = Duration::from_millis(250);

        let size = self.channel.size;
        let topic = topics::publishers_to_hub_stream_topic(
            self.namespace.clone(),
            self.channel.name.clone(),
        );

        let channel_size = self.channel.size;
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
                            .handle_raw_stream_message_from_publishers(stream_message, channel_size)
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

    pub async fn get_snapshot(&mut self) -> Vec<StreamMessage> {
        let keys = self.get_snapshot_keys().await;
        debug!("keys are here {:?}", keys);

        if keys.len() == 0 {
            return Vec::new();
        }

        let mut messages = Vec::new();

        let ids = vec![0; keys.len()];
        debug!("ids are key {:?}", ids);

        let mut options = XReadOptions::default();
        options = options.count(self.channel.size);

        let results: Vec<(String, Vec<StreamEntry<String>>)> = self
            .redis
            .as_mut()
            .unwrap()
            .lock()
            .await
            .xread(options, keys, ids)
            .await
            .unwrap();

        for (_key, entries) in results {
            for entry in entries {
                for (_key, value) in entry.items.into_iter() {
                    let raw: StreamMessage = serde_json::from_str(value.as_str()).unwrap();
                    messages.push(raw);
                }
            }
        }

        debug!("message count {:?}", messages.len());

        messages
    }

    async fn get_snapshot_keys(&mut self) -> Vec<String> {
        let snapshot_topic = topics::hub_channel_snapshot_topic(
            self.namespace.clone(),
            self.channel.name.clone(),
            String::from("*"),
            String::from("*"),
        );

        let mut count = 0u64;
        let mut keys: Vec<String> = vec![];

        loop {
            let mut options = ScanOptions::default();
            options = options.match_pattern(snapshot_topic.clone());
            let result = self
                .redis
                .as_mut()
                .unwrap()
                .lock()
                .await
                .scan(count, options)
                .await;

            if result.is_ok() {
                let mut entries: (u64, Vec<String>) = result.unwrap();
                count = entries.0;
                keys.append(&mut entries.1);

                if count == 0 {
                    break;
                }
            } else {
                break;
            }
        }

        debug!("found {} keys", keys.len());

        keys
    }
}

impl Drop for StreamingChannel {
    fn drop(&mut self) {
        if self.join_handler.is_some() {
            self.join_handler.as_mut().unwrap().abort();
        }
    }
}
