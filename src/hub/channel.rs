use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use crate::hub::messages::MessageHandler;
use log::{debug, trace, warn};
use redis::streams::StreamKey;
use redis::streams::StreamReadReply;
use redis::streams::{StreamId, StreamReadOptions};
use redis::Commands;
use redis::RedisResult;
use rhiaqey_common::redis::RedisSettings;
use rhiaqey_common::redis_rs::connect;
use rhiaqey_common::stream::StreamMessage;
use rhiaqey_common::topics;
use rhiaqey_common::RhiaqeyResult;
use rhiaqey_sdk_rs::channel::Channel;

use tokio::task::JoinHandle;

pub struct StreamingChannel {
    pub hub_id: String,
    pub channel: Channel,
    pub namespace: String,
    pub redis: Arc<Mutex<redis::Connection>>,
    pub message_handler: Arc<Mutex<MessageHandler>>,
    pub join_handler: Option<Arc<JoinHandle<u32>>>,
    pub clients: Arc<RwLock<Vec<String>>>,
}

impl StreamingChannel {
    pub fn create(
        hub_id: String,
        namespace: String,
        channel: Channel,
        config: RedisSettings,
    ) -> RhiaqeyResult<StreamingChannel> {
        let redis_rs_client = connect(&config)?;
        let redis_rs_connection = redis_rs_client.get_connection()?;
        let redis_ms_connection = redis_rs_client.get_connection()?;
        let rx = Arc::new(Mutex::new(redis_rs_connection));

        Ok(StreamingChannel {
            hub_id: hub_id.clone(),
            channel: channel.clone(),
            namespace: namespace.clone(),
            redis: rx.clone(),
            clients: Arc::new(RwLock::new(vec![])),
            join_handler: None,
            message_handler: Arc::new(Mutex::new(MessageHandler::create(
                hub_id.clone(),
                channel.clone(),
                namespace.clone(),
                redis_ms_connection,
            ))),
        })
    }

    fn read_group_records(
        redis_rs_connection: Arc<Mutex<redis::Connection>>,
        hub_id: &String,
        channel: &Channel,
        namespace: &String,
    ) -> RhiaqeyResult<Vec<StreamMessage>> {
        let mut connection = redis_rs_connection.lock().unwrap();

        let options = StreamReadOptions::default()
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

    pub fn start(&mut self) {
        let hub_id = self.get_hub_id();
        let channel = self.channel.clone();
        let namespace = self.namespace.clone();
        let lock = self.redis.clone();
        let message_handler = self.message_handler.clone();

        let join_handler = tokio::task::spawn(async move {
            loop {
                for entry in Self::read_group_records(lock.clone(), &hub_id, &channel, &namespace)
                    .unwrap_or(vec![])
                {
                    match message_handler
                        .lock()
                        .unwrap()
                        .handle_stream_message_from_publishers(entry)
                    {
                        Ok(_) => trace!("successfully handled raw message"),
                        Err(err) => warn!("error handling raw stream message: {}", err),
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
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

    pub fn add_client(&mut self, connection_id: String) {
        self.clients.write().unwrap().push(connection_id);
    }

    pub fn get_snapshot(&mut self) -> RhiaqeyResult<Vec<StreamMessage>> {
        let keys = self.get_snapshot_keys()?;
        debug!("keys are here {:?}", keys);

        if keys.len() == 0 {
            return Ok(vec![]);
        }

        let ids = vec![0; keys.len()];
        debug!("ids are key {:?}", ids);

        let options = StreamReadOptions::default().count(self.channel.size);

        let lock = self.redis.clone();
        let mut client = lock.lock().unwrap();
        let results: StreamReadReply = client.xread_options(&*keys, &*ids, &options)?;

        let messages: Vec<StreamMessage> = results
            .keys
            .iter()
            .map(|key| {
                key.ids
                    .iter()
                    .map(|id| id.map.get("raw"))
                    .filter(|x| x.is_some())
                    .map(|x| x.unwrap())
                    .filter_map(|x| {
                        return if let redis::Value::Data(ref data) = x {
                            Some(data)
                        } else {
                            None
                        };
                    })
                    .filter_map(|x| serde_json::from_slice::<StreamMessage>(x).ok())
            })
            .flatten()
            .collect();

        debug!("message count {:?}", messages.len());

        Ok(messages)
    }

    pub fn get_snapshot_keys(&mut self) -> RhiaqeyResult<Vec<String>> {
        let snapshot_topic = topics::hub_channel_snapshot_topic(
            self.namespace.clone(),
            self.channel.name.to_string(),
            String::from("*"),
            String::from("*"),
        );

        let lock = self.redis.clone();
        let mut client = lock.lock().unwrap();
        let keys: Vec<String> = client.scan_match(&snapshot_topic)?.collect();
        debug!("found {} keys", keys.len());

        Ok(keys)
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
            self.redis.clone(),
            &self.hub_id,
            &self.channel,
            &self.namespace,
        )
        .unwrap_or(vec![]);
        std::thread::sleep(Duration::from_millis(100));
        return Poll::Ready(Some(items));
    }
}
