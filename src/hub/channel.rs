use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use crate::hub::messages::MessageHandler;
use log::{debug, trace, warn};
use redis::streams::StreamId;
use redis::streams::StreamKey;
use redis::streams::StreamReadReply;
use redis::Commands;
use redis::RedisResult;
use rhiaqey_common::redis::connect_and_ping_async;
use rhiaqey_common::redis::RedisSettings;
use rhiaqey_common::redis_rs::connect;
use rhiaqey_common::stream::StreamMessage;
use rhiaqey_common::topics;
use rhiaqey_common::RhiaqeyResult;
use rhiaqey_sdk_rs::channel::Channel;
use rustis::client::Client;
use rustis::commands::{
    GenericCommands, ScanOptions, StreamCommands, StreamEntry, XReadGroupOptions, XReadOptions,
};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

pub struct StreamingChannel {
    pub hub_id: String,
    pub channel: Channel,
    pub namespace: String,
    pub redis_rs_connection: Arc<std::sync::Mutex<redis::Connection>>,
    pub redis: Option<Arc<Mutex<Client>>>,
    pub message_handler: Arc<std::sync::Mutex<MessageHandler>>,
    pub join_handler: Option<Arc<JoinHandle<u32>>>,
    pub clients: Arc<Mutex<Vec<String>>>,
}

impl StreamingChannel {
    pub async fn create(
        hub_id: String,
        namespace: String,
        channel: Channel,
        config: RedisSettings,
    ) -> RhiaqeyResult<StreamingChannel> {
        let redis_rs_client = connect(&config)?;
        let redis_rs_connection = redis_rs_client.get_connection()?;
        let redis_connection = connect_and_ping_async(config.clone()).await?;
        let rx = Arc::new(std::sync::Mutex::new(redis_rs_connection));

        Ok(StreamingChannel {
            hub_id: hub_id.clone(),
            channel: channel.clone(),
            namespace: namespace.clone(),
            redis_rs_connection: rx.clone(),
            clients: Arc::new(Mutex::new(vec![])),
            join_handler: None,
            redis: Some(Arc::new(Mutex::new(redis_connection))),
            message_handler: Arc::new(std::sync::Mutex::new(MessageHandler::create(
                hub_id.clone(),
                channel.clone(),
                namespace.clone(),
                rx.clone(),
            ))),
        })
    }

    fn read_group_records(
        redis_rs_connection: Arc<std::sync::Mutex<redis::Connection>>,
        hub_id: &String,
        channel: &Channel,
        namespace: &String,
    ) -> RhiaqeyResult<Vec<StreamMessage>> {
        let mut connection = redis_rs_connection.try_lock().unwrap();

        let options = redis::streams::StreamReadOptions::default()
            .count(channel.size)
            .group("hub", hub_id);

        let topic = topics::publishers_to_hub_stream_topic(namespace, &channel.name.to_string());

        let reply: StreamReadReply = connection.xread_options(&[topic], &[">"], &options)?;

        let mut entries: Vec<StreamMessage> = vec![];

        for StreamKey { key, ids } in reply.keys {
            for StreamId { id: _, map } in &ids {
                if let Some(raw) = map.get("raw") {
                    if let redis::Value::Data(ref data) = raw {
                        if let Ok(entry) = serde_json::from_slice::<StreamMessage>(data) {
                            trace!(
                                "found entry key={}, timestamp={:?}",
                                entry.key,
                                entry.timestamp
                            );
                            entries.push(entry);
                        }
                    }
                }
            }

            // acknowledge each stream and message ID once all messages are
            let keys: Vec<&String> = ids.iter().map(|StreamId { id, map: _ }| id).collect();
            let _: RedisResult<i32> = connection.xack(key, "hub", &keys);
        }

        Ok(entries)
    }

    pub async fn start(&mut self) {
        let id = self.get_hub_id();
        let channel = self.channel.clone();
        let namespace = self.namespace.clone();
        let duration = Duration::from_millis(150);

        debug!("arxizei to match");

        let redis = self.redis.as_mut().unwrap().clone();
        let message_handler = self.message_handler.clone();

        let join_handler = tokio::task::spawn(async move {
            let id = id.clone();
            let topic = topics::publishers_to_hub_stream_topic(namespace, channel.name.to_string());

            loop {
                let lxd = redis.lock().await;

                let results: rustis::Result<Vec<(String, Vec<StreamEntry<String>>)>> = lxd
                    .xreadgroup(
                        "hub",
                        id.clone(),
                        XReadGroupOptions::default().count(channel.size),
                        topic.clone(),
                        ">",
                    )
                    .await;

                if let Err(e) = results {
                    warn!("error with retrieving results: {}", e);
                    drop(lxd);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }

                let mut ids: Vec<String> = vec![];

                for (_ /* topic */, items) in results.unwrap().iter() {
                    for item in items.iter() {
                        ids.push(item.stream_id.clone());
                        if let Some(raw) = item.items.get("raw") {
                            if let Ok(stream_message) = serde_json::from_str::<StreamMessage>(raw) {
                                let _ = message_handler
                                    .lock()
                                    .unwrap()
                                    .handle_stream_message_from_publishers(stream_message);
                            }
                        }
                    }
                }

                if ids.len() == 0 {
                    drop(lxd);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }

                trace!("must ack {} stream ids", ids.len());

                let result = match lxd.xack(topic.clone(), "hub", ids.clone()).await {
                    Ok(res) => {
                        trace!("ack {res} stream ids");
                        true
                    }
                    Err(e) => {
                        warn!("failed to ack stream ids {e}");
                        false
                    }
                };

                if result {
                    trace!("must delete {} stream ids", ids.len());
                    match lxd.xdel(topic.clone(), ids).await {
                        Ok(res) => {
                            debug!("received {res} stream messages");
                        }
                        Err(e) => {
                            warn!("failed to del stream ids {e}");
                        }
                    };
                }

                drop(lxd);
                tokio::time::sleep(duration).await;
            }
        });

        self.join_handler = Some(Arc::new(join_handler));
    }

    pub fn get_hub_id(&self) -> String {
        return self.hub_id.to_string();
    }

    pub fn get_name(&self) -> String {
        return self.channel.name.to_string();
    }

    pub async fn add_client(&mut self, connection_id: String) {
        self.clients.lock().await.push(connection_id);
    }

    pub async fn get_snapshot(&mut self) -> Vec<StreamMessage> {
        let keys = self.get_snapshot_keys().await;
        debug!("keys are here {:?}", keys);

        if keys.len() == 0 {
            return vec![];
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
                for (key, value) in entry.items.into_iter() {
                    if key.eq("raw") {
                        let raw: StreamMessage = serde_json::from_str(value.as_str()).unwrap();
                        messages.push(raw);
                    }
                }
            }
        }

        debug!("message count {:?}", messages.len());

        messages
    }

    pub async fn get_snapshot_keys(&mut self) -> Vec<String> {
        let snapshot_topic = topics::hub_channel_snapshot_topic(
            self.namespace.clone(),
            self.channel.name.to_string(),
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

impl futures::stream::Stream for StreamingChannel {
    type Item = Vec<StreamMessage>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let items = Self::read_group_records(
            self.redis_rs_connection.clone(),
            &self.hub_id,
            &self.channel,
            &self.namespace,
        )
        .unwrap_or(vec![]);
        std::thread::sleep(Duration::from_millis(100));
        return Poll::Ready(Some(items));
    }
}
