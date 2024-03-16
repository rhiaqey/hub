use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::hub::messages::MessageHandler;
use log::{debug, trace, warn};
use redis::streams::{StreamId, StreamKey, StreamMaxlen, StreamRangeReply, StreamReadReply};
use redis::{Commands, RedisResult, Value};
use rhiaqey_common::error::RhiaqeyError;
use rhiaqey_common::pubsub::{RPCMessage, RPCMessageData};
use rhiaqey_common::redis::{connect_and_ping_async, RedisSettings};
use rhiaqey_common::redis_rs::connect;
use rhiaqey_common::stream::StreamMessage;
use rhiaqey_common::topics;
use rhiaqey_sdk_rs::channel::Channel;
use rustis::client::Client;
use rustis::commands::{
    GenericCommands, ScanOptions, StreamCommands, StreamEntry, XReadGroupOptions, XReadOptions,
};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

#[derive(PartialEq)]
enum MessageProcessResult {
    Allow(StreamMessage),
    AllowUnprocessed,
    Deny(String),
    CheckIfMessageExists,
}

pub struct StreamingChannel {
    hub_id: String,
    pub channel: Channel,
    pub namespace: String,
    pub client: Option<redis::Client>,
    pub redis: Option<Arc<Mutex<Client>>>,
    pub message_handler: Option<Arc<Mutex<MessageHandler>>>,
    join_handler: Option<Arc<JoinHandle<u32>>>,
    pub clients: Arc<Mutex<Vec<String>>>,
}

impl StreamingChannel {
    pub async fn create(hub_id: String, namespace: String, channel: Channel) -> StreamingChannel {
        StreamingChannel {
            hub_id,
            channel,
            namespace,
            client: None,
            redis: None,
            message_handler: None,
            join_handler: None,
            clients: Arc::new(Mutex::new(vec![])),
        }
    }

    pub async fn setup(&mut self, config: RedisSettings) -> Result<(), RhiaqeyError> {
        let settings = config.clone();
        let client = connect(&settings)?;
        let connection = connect_and_ping_async(config.clone()).await?;
        self.client = Some(client);
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
        Ok(())
    }

    fn read_group_records(&self) -> Result<Vec<StreamMessage>, RhiaqeyError> {
        let mut connection = self.client.as_ref().unwrap().get_connection()?;

        let options = redis::streams::StreamReadOptions::default()
            .count(self.channel.size)
            .group("hub", self.get_hub_id());

        let namespace = self.namespace.clone();
        let channel_name = self.channel.name.to_string();

        let topic = topics::publishers_to_hub_stream_topic(namespace, channel_name);

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

    fn compare_by_tags(
        &mut self,
        new_msg: &StreamMessage,
        old_msg: &StreamMessage,
    ) -> MessageProcessResult {
        trace!("checking if message should be processed (compare by tags)");

        let new_tag = new_msg.tag.clone().unwrap_or(String::from(""));
        let old_tag = old_msg.tag.clone().unwrap_or(String::from(""));

        if old_tag.is_empty() && new_tag.is_empty() {
            return MessageProcessResult::AllowUnprocessed;
        }

        if old_tag == new_tag {
            return MessageProcessResult::Deny(String::from("due to same tags"));
        }

        MessageProcessResult::AllowUnprocessed
    }

    fn compare_against_all(
        &self,
        new_message: &StreamMessage,
        topic: &String,
    ) -> Result<MessageProcessResult, RhiaqeyError> {
        trace!("checking if message should be processed 1-to-many (compare by tags)");
        trace!("checking topic {}", topic);

        let new_message = new_message.clone();
        let new_tag = new_message.tag.unwrap_or("".to_string());

        let mut connection = self.client.clone().unwrap().get_connection()?;

        let results: StreamRangeReply =
            connection.xrevrange_count(topic, "+", "-", self.channel.size)?;

        if results.ids.len() == 0 {
            // allow it since we have not stored data to compare against
            return Ok(MessageProcessResult::AllowUnprocessed);
        }

        trace!("found {}", results.ids.len());

        // Checking all results
        for entry in results.ids.iter() {
            if let Some(stored_tag) = entry.map.get("tag") {
                if let Value::Data(bytes) = stored_tag {
                    if let Ok(old_tag) = String::from_utf8(bytes.clone()) {
                        if new_tag == old_tag {
                            warn!("tag \"{new_tag}\" already found stored");
                            return Ok(MessageProcessResult::Deny(String::from(
                                "tag already found",
                            )));
                        }
                    }
                }
            }
        }

        Ok(MessageProcessResult::AllowUnprocessed)
    }

    fn compare_by_timestamp(
        &mut self,
        topic: &String,
        new_message: &StreamMessage,
    ) -> Result<MessageProcessResult, RhiaqeyError> {
        trace!("checking if message should be processed (compare by timestamps)");
        trace!("checking topic {}", topic);

        // 1. if the new message has timestamp=0 means do not check at all.
        //    Let it pass through.
        let new_timestamp = new_message.timestamp.unwrap_or(0);
        if new_timestamp == 0 {
            debug!("new message has timestamp = 0");
            // allow it without checking
            return Ok(MessageProcessResult::AllowUnprocessed);
        }

        let mut connection = self.client.clone().unwrap().get_connection()?;
        let results: StreamRangeReply = connection.xrevrange_count(topic, "+", "-", 1)?;

        if results.ids.len() == 0 {
            // allow it since we have not stored data to compare against
            return Ok(MessageProcessResult::AllowUnprocessed);
        }

        trace!("found {}", results.ids.len());

        // Checking only the first result
        let last_entry = results.ids.iter().next().unwrap();

        let Some(last_message) = last_entry.map.get("raw") else {
            warn!("last message in raw not found");
            // allow it as the stored one did not have a correct format
            return Ok(MessageProcessResult::AllowUnprocessed);
        };

        let Value::Data(bytes) = last_message else {
            return Err("could not extract bytes from last message".into());
        };

        let msg = String::from_utf8(bytes.clone())?;
        let decoded = StreamMessage::der_from_string(msg.as_str())?;

        // =========================================================================================

        let old_timestamp = decoded.timestamp.unwrap_or(0);

        // 2. If old timestamp is more recent, do not process the new one.
        if old_timestamp > new_timestamp {
            // do not allow it as it is not fresh data
            return Ok(MessageProcessResult::Deny(String::from("old timestamp")));
        }

        // 3. If new timestamp has the same with the stored one, then we need to check if a message
        //    exists in the whole list. Must compare against all stored messages and compare tags.
        if old_timestamp == new_timestamp {
            // we need to further examine the message
            return Ok(MessageProcessResult::CheckIfMessageExists);
        }

        trace!(
            "clear to proceed as timestamp is valid {} - {} - {}",
            old_timestamp,
            new_timestamp,
            old_timestamp > new_timestamp
        );

        // We are allowing anything else that does not meet out criteria to
        // deny processing the message.
        Ok(MessageProcessResult::Allow(decoded))
    }

    fn handle_raw_stream_message_from_publishers(
        &mut self,
        stream_message: StreamMessage,
    ) -> Result<(), RhiaqeyError> {
        debug!("handle raw stream message");

        let channel_size = stream_message.size.unwrap_or(self.channel.size) as usize;
        let mut new_message = stream_message.clone();
        new_message.hub_id = Some(self.hub_id.clone());

        let snapshot_topic = topics::hub_channel_snapshot_topic(
            self.namespace.clone(),
            stream_message.channel.clone().into(),
            stream_message.key.clone(),
            stream_message
                .category
                .clone()
                .unwrap_or(String::from("default")),
        );

        let compare_timestamps = self.compare_by_timestamp(&snapshot_topic, &stream_message)?;

        if let MessageProcessResult::Deny(reason) = compare_timestamps {
            warn!("raw message should not be processed further due to: {reason}");
            return Ok(());
        }

        if let MessageProcessResult::CheckIfMessageExists = compare_timestamps {
            trace!("must compare message against all");

            let compare_against_all = self.compare_against_all(&new_message, &snapshot_topic)?;

            if let MessageProcessResult::Deny(reason) = compare_against_all {
                warn!("raw message should not be processed further due to: {reason}");
                return Ok(());
            }

            trace!("message tags are not same 1-to-many");
        }

        if let MessageProcessResult::Allow(ref old_message) = compare_timestamps {
            trace!("allowing message to proceed (check by tags)");

            let compare_tags = self.compare_by_tags(&new_message, old_message);

            if let MessageProcessResult::Deny(reason) = compare_tags {
                warn!("raw message should not be processed further due to: {reason}");
                return Ok(());
            }

            trace!("message tags are not same");
        }

        if let MessageProcessResult::AllowUnprocessed = compare_timestamps {
            trace!("allowing message to proceed unprocessed");
        }

        let Ok(raw_message) = stream_message.ser_to_string() else {
            warn!("failed to message to string");
            return Ok(());
        };

        let clean_topic = topics::hub_raw_to_hub_clean_pubsub_topic(self.namespace.clone());
        let message = RPCMessage {
            data: RPCMessageData::NotifyClients(new_message),
        };

        // Prepare to broadcast to all hubs that we have clean message
        let raw = message.ser_to_string()?;

        let mut connection = self.client.clone().unwrap().get_connection()?;
        connection.publish(&clean_topic, raw)?;
        trace!("message sent to pubsub {}", &clean_topic);

        let tag = stream_message.tag.unwrap_or(String::from(""));
        let mut items = BTreeMap::new();
        items.insert("raw", raw_message);
        items.insert("tag", tag);

        let id = connection.xadd_maxlen_map(
            snapshot_topic.clone(),
            StreamMaxlen::Equals(channel_size),
            "*",
            items,
        )?;
        trace!("message sent to clean xstream {}: {:?}", &clean_topic, id);

        Ok(())
    }

    pub async fn start(&mut self) {
        let id = self.get_hub_id();
        let channel = self.channel.clone();
        let namespace = self.namespace.clone();
        let duration = Duration::from_millis(150);

        let redis = self.redis.as_mut().unwrap().clone();
        let message_handler = self.message_handler.as_mut().unwrap().clone();

        for entry in self.read_group_records().unwrap_or(vec![]) {
            match self.handle_raw_stream_message_from_publishers(entry) {
                Ok(_) => trace!("successfully handled raw message"),
                Err(err) => warn!("error handling raw stream message: {}", err),
            }
        }

        /*
        match self.handle_raw_stream_message_from_publishers(entry) {
            Ok(_) => trace!("successfully handled raw message"),
            Err(err) => warn!("error handling raw stream message: {}", err),
        }*/
        /*
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
                                    .await
                                    .handle_raw_stream_message_from_publishers_async(
                                        stream_message,
                                        channel.size,
                                    )
                                    .await;
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

        self.join_handler = Some(Arc::new(join_handler));*/
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
