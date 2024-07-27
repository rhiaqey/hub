use anyhow::bail;
use axum::extract::ws::Message;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use crate::http::websockets::SnapshotParam;
use crate::hub::client::WebSocketClient;
use crate::hub::messages::MessageHandler;
use log::{debug, trace, warn};
use redis::streams::StreamReadReply;
use redis::streams::{StreamId, StreamReadOptions};
use redis::streams::{StreamKey, StreamMaxlen};
use redis::Commands;
use redis::RedisResult;
use rhiaqey_common::client::ClientMessage;
use rhiaqey_common::redis::RedisSettings;
use rhiaqey_common::redis_rs::connect_and_ping;
use rhiaqey_common::stream::StreamMessage;
use rhiaqey_common::topics;
use rhiaqey_sdk_rs::channel::Channel;

pub struct StreamingChannel {
    hub_id: String,
    channel: Channel,
    namespace: String,
    redis: Arc<Mutex<redis::Connection>>,
    message_handler: Arc<Mutex<MessageHandler>>,
    last_message: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    pub clients: Arc<RwLock<Vec<String>>>,
}

impl StreamingChannel {
    pub fn create(
        hub_id: String,
        namespace: String,
        channel: Channel,
        config: RedisSettings,
    ) -> anyhow::Result<StreamingChannel> {
        let redis_rs_client = connect_and_ping(&config)?;
        let redis_rs_connection = redis_rs_client.get_connection()?;
        let redis_ms_connection = redis_rs_client.get_connection()?;
        let rx = Arc::new(Mutex::new(redis_rs_connection));

        Ok(StreamingChannel {
            hub_id: hub_id.clone(),
            channel: channel.clone(),
            namespace: namespace.clone(),
            redis: rx.clone(),
            message_handler: Arc::new(Mutex::new(MessageHandler::create(
                hub_id.clone(),
                channel.clone(),
                namespace.clone(),
                redis_ms_connection,
            ))),
            clients: Arc::new(RwLock::new(vec![])),
            last_message: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    fn read_group_records(
        redis_rs_connection: Arc<Mutex<redis::Connection>>,
        hub_id: &String,
        channel: &Channel,
        namespace: &String,
    ) -> anyhow::Result<Vec<StreamMessage>> {
        let mut connection = redis_rs_connection.lock().unwrap();

        let options = StreamReadOptions::default()
            .count(channel.size)
            .group("hub", hub_id);

        let topic = topics::publishers_to_hub_stream_topic(namespace, &channel.name.to_string());

        let reply: StreamReadReply =
            connection.xread_options(&[topic.clone()], &[">"], &options)?;

        let mut entries: Vec<StreamMessage> = vec![];

        for StreamKey { key, ids } in reply.keys {
            for StreamId { id: _, map } in &ids {
                if let Some(raw) = map.get("raw") {
                    if let redis::Value::BulkString(m) = raw {
                        trace!("raw is bulk string {:?}", m);
                        trace!(
                            "raw json dump {:?}",
                            serde_json::from_slice::<StreamMessage>(m.as_slice()).unwrap()
                        );
                    }

                    if let redis::Value::SimpleString(ref data) = raw {
                        if let Ok(entry) = serde_json::from_str::<StreamMessage>(data) {
                            trace!(
                                "found entry key={}, timestamp={:?}",
                                entry.key,
                                entry.timestamp
                            );
                            entries.push(entry);
                        }
                    } else {
                        trace!("raw is not a simple string")
                    }
                } else {
                    trace!("raw not found in stream entry");
                }
            }

            // acknowledge each stream and message ID once all messages are
            let keys: Vec<&String> = ids.iter().map(|StreamId { id, map: _ }| id).collect();
            debug!("need to ack {} stream keys", keys.len());

            let ack_result: RedisResult<i32> = connection.xack(key, "hub", &keys);
            match ack_result {
                Ok(count) => debug!("ack {} records for topic {}", count, topic),
                Err(err) => warn!("error ack {} keys for topic {}: {}", keys.len(), topic, err),
            }
        }

        Ok(entries)
    }

    pub fn start(&mut self) {
        let hub_id = self.get_hub_id();
        let channel = self.channel.clone();
        let namespace = self.namespace.clone();
        let lock = self.redis.clone();
        let message_handler = self.message_handler.clone();

        tokio::task::spawn(async move {
            loop {
                for entry in Self::read_group_records(lock.clone(), &hub_id, &channel, &namespace)
                    .unwrap_or(vec![])
                    .iter()
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
    }

    pub fn get_hub_id(&self) -> String {
        return self.hub_id.to_string();
    }

    pub fn get_channel(&self) -> &Channel {
        &self.channel
    }

    pub fn add_client(&mut self, connection_id: String) {
        self.clients.write().unwrap().push(connection_id);
    }

    pub fn remove_client(&mut self, connection_id: String) {
        self.clients
            .write()
            .unwrap()
            .retain(|x| x != &connection_id);
    }

    pub fn get_total_clients(&self) -> usize {
        self.clients.read().unwrap().len()
    }

    pub fn get_snapshot(
        &mut self,
        snapshot_param: &SnapshotParam,
        category: Option<String>,
        key: Option<String>,
        count: Option<usize>,
    ) -> anyhow::Result<Vec<StreamMessage>> {
        let keys = self.get_snapshot_keys(category, key)?;
        debug!("keys are here {:?}", keys);

        if keys.len() == 0 {
            return Ok(vec![]);
        }

        let ids = vec![0; keys.len()];
        debug!("ids are key {:?}", ids);

        let count = count.unwrap_or(self.channel.size);
        if count == 0 {
            // If size is return 0 then return early
            return Ok(vec![]);
        }

        let options = StreamReadOptions::default().count(count);
        debug!("stream read options: {:?}", options);

        let lock = self.redis.clone();
        let mut client = lock.lock().unwrap();
        let results: StreamReadReply = client.xread_options(&*keys, &*ids, &options)?;

        let mut messages: Vec<StreamMessage> = results
            .keys
            .iter()
            .map(|key| {
                key.ids
                    .iter()
                    .map(|id| id.map.get("raw"))
                    .filter(|x| x.is_some())
                    .map(|x| x.unwrap())
                    .filter_map(|x| {
                        return if let redis::Value::SimpleString(ref data) = x {
                            Some(data)
                        } else {
                            None
                        };
                    })
                    .filter_map(|x| serde_json::from_str::<StreamMessage>(x).ok())
            })
            .flatten()
            .collect();

        debug!("message count {:?}", messages.len());

        if let SnapshotParam::DESC = snapshot_param {
            messages.reverse()
        }

        Ok(messages)
    }

    pub fn get_snapshot_keys(
        &mut self,
        category: Option<String>,
        key: Option<String>,
    ) -> anyhow::Result<Vec<String>> {
        let snapshot_topic = topics::hub_channel_snapshot_topic(
            self.namespace.clone(),
            self.channel.name.to_string(),
            String::from(category.unwrap_or(String::from("*"))),
            key.unwrap_or(String::from("*")),
        );
        trace!("snapshot key topic: {}", snapshot_topic);

        let lock = self.redis.clone();
        let mut client = lock.lock().unwrap();
        let keys: Vec<String> = client.scan_match(&snapshot_topic)?.collect();
        debug!("found {} scan keys", keys.len());

        Ok(keys)
    }

    pub fn delete_snapshot_keys(
        &mut self,
        category: Option<String>,
        key: Option<String>,
    ) -> anyhow::Result<i32> {
        let keys = self.get_snapshot_keys(category, key).unwrap_or(vec![]);
        trace!("{} keys found", keys.len());

        let lock = self.redis.clone();
        let mut client = lock.lock().unwrap();

        let result: i32 = client.del(keys).unwrap_or(0);

        Ok(result)
    }

    pub fn xtrim(&self) -> anyhow::Result<i32> {
        let topic = topics::publishers_to_hub_stream_topic(
            self.namespace.clone(),
            self.channel.name.to_string(),
        );

        let lock = self.redis.clone();
        let mut client = lock.lock().unwrap();

        let result: i32 = client.xtrim(topic, StreamMaxlen::Equals(0)).unwrap_or(0);
        debug!("trimmed {} entries", result);

        Ok(result)
    }

    pub fn set_last_client_message(&self, message: Vec<u8>, category: Option<String>) {
        let mut msg = self.last_message.write().unwrap();
        msg.insert(category.unwrap_or(String::from("default")), message);
    }

    pub fn get_last_client_message(&self, category: Option<String>) -> Option<Vec<u8>> {
        let lock = self.last_message.clone();
        let raw = lock.read().unwrap();
        let result = raw.get(&category.unwrap_or(String::from("default")));
        result.cloned()
    }

    #[inline(always)]
    async fn send_message_to_client(
        &self,
        client: &mut WebSocketClient,
        message_key: &String,
        message_category: &Option<String>,
        channel_name: &String,
        message: Vec<u8>,
    ) -> anyhow::Result<()> {
        if let Some((cat, client_channel_key)) = client.get_category_for_channel(&channel_name) {
            // NOTE:
            // We want only to compare if the cat (category specified in the channel) has a value.
            // if it is empty, then we do not care about the category, and we allow all.
            if cat.is_some() && !message_category.eq(cat) {
                bail!(
                    "skipping message broadcast as the categories do not match: {:?} vs {:?}",
                    message_category,
                    cat
                )
            }

            if let Some(channel_key) = client_channel_key {
                if !channel_key.eq(message_key) {
                    bail!(
                        "skipping message broadcast as the keys do not match: {:?}",
                        message_key
                    )
                }
            }
        }

        client.send(Message::Binary(message)).await
    }

    pub async fn broadcast(
        &mut self,
        stream_message: StreamMessage,
        clients: Arc<tokio::sync::Mutex<HashMap<String, WebSocketClient>>>,
    ) -> anyhow::Result<()> {
        let total_channel_clients = self.get_total_clients();

        debug!(
            "broadcasting message from channel {} to {} clients",
            self.get_channel().name,
            total_channel_clients
        );

        if total_channel_clients == 0 {
            return Ok(());
        }

        let channel_name = &self.channel.name;
        trace!("streaming channel found {}", channel_name);

        let key = stream_message.key.clone();
        let category = stream_message.category.clone();
        let user_ids = stream_message.user_ids.clone();
        let client_ids = stream_message.client_ids.clone();

        let mut client_message = ClientMessage::from(stream_message);

        if cfg!(debug_assertions) {
            if client_message.hub_id.is_none() {
                client_message.hub_id = Some(self.get_hub_id());
            }
        } else {
            client_message.hub_id = None;
            client_message.publisher_id = None;
        }

        let raw = rmp_serde::to_vec_named(&client_message)?;
        self.set_last_client_message(raw.clone(), client_message.category);
        trace!(
            "last message cached in streaming channel[name={}]",
            channel_name
        );

        let all_stream_channel_clients = self.clients.read().unwrap();
        let mut all_hub_clients = clients.lock().await;
        let total_hub_clients = all_hub_clients.len();

        let mut total_sent_messages = 0u32;

        // If we are not using the user ids and client ids fields
        for client_id in all_stream_channel_clients.iter().filter(|x| {
            match &client_ids {
                None => true, // no client ids are specified, so we do not filter anything
                Some(clients) => clients.contains(*x),
            }
        }) {
            match all_hub_clients.get_mut(client_id) {
                Some(client) => {
                    match &user_ids {
                        None => {} // do nothing, proceed
                        Some(users) => {
                            if let Some(user_id) = client.get_user_id() {
                                if users.contains(user_id) {
                                    trace!("valid user if found: {}", user_id);
                                    // do nothing, proceed
                                } else {
                                    trace!("user id was not in the whitelisted users");
                                    continue; // skip
                                }
                            } else {
                                trace!("client does not have a user id to compare;");
                                continue; // skip
                            }
                        }
                    }

                    trace!(
                        "sending message to client: {}[user_id={:?}]",
                        client.get_client_id(),
                        client.get_user_id()
                    );

                    match self
                        .send_message_to_client(client, &key, &category, channel_name, raw.clone())
                        .await
                    {
                        Ok(_) => {
                            trace!("message sent successfully to client {client_id}");
                            total_sent_messages += 1;
                        }
                        Err(err) => {
                            warn!("message was not sent: {}", err);
                        }
                    }
                }
                None => warn!("failed to find client by id {client_id}"),
            }
        }

        debug!(
            "notified {}/{}/{} clients",
            total_sent_messages, total_channel_clients, total_hub_clients
        );

        Ok(())
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
