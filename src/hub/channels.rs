use std::collections::BTreeMap;

use log::{debug, trace, warn};
use redis::streams::{
    StreamId, StreamKey, StreamMaxlen, StreamRangeReply, StreamReadOptions, StreamReadReply,
};
use redis::{Commands, RedisResult, Value};

use rhiaqey_common::pubsub::{RPCMessage, RPCMessageData};
use rhiaqey_common::redis::RedisSettings;
use rhiaqey_common::redis_rs::connect;
use rhiaqey_common::stream::StreamMessage;
use rhiaqey_common::{topics, RhiaqeyResult};
use rhiaqey_sdk_rs::channel::Channel;

use tokio::sync::Mutex;

#[derive(PartialEq)]
enum MessageProcessResult {
    Allow(StreamMessage),
    AllowUnprocessed,
    Deny(String),
    CheckIfMessageExists,
}

pub struct StreamingChannel {
    pub hub_id: String,
    pub channel: Channel,
    pub namespace: String,
    pub client: redis::Connection,
    pub clients: Mutex<Vec<String>>,
}

impl StreamingChannel {
    pub fn create(
        hub_id: String,
        namespace: String,
        channel: Channel,
        settings: &RedisSettings,
    ) -> RhiaqeyResult<StreamingChannel> {
        let client = connect(settings)?;
        let connection = client.get_connection()?;
        Ok(StreamingChannel {
            hub_id,
            channel,
            namespace,
            client: connection,
            clients: Mutex::new(vec![]),
        })
    }

    fn read_group_records(&mut self) -> RhiaqeyResult<Vec<StreamMessage>> {
        let options = redis::streams::StreamReadOptions::default()
            .count(self.channel.size)
            .group("hub", self.get_hub_id());

        let namespace = self.namespace.clone();
        let channel_name = self.channel.name.to_string();

        let topic = topics::publishers_to_hub_stream_topic(namespace, channel_name);

        let reply: StreamReadReply = self.client.xread_options(&[topic], &[">"], &options)?;

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
            let _: RedisResult<i32> = self.client.xack(key, "hub", &keys);
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
        &mut self,
        new_message: &StreamMessage,
        topic: &String,
    ) -> RhiaqeyResult<MessageProcessResult> {
        trace!("checking if message should be processed 1-to-many (compare by tags)");
        trace!("checking topic {}", topic);

        let new_message = new_message.clone();
        let new_tag = new_message.tag.unwrap_or("".to_string());

        let results: StreamRangeReply =
            self.client
                .xrevrange_count(topic, "+", "-", self.channel.size)?;

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
    ) -> RhiaqeyResult<MessageProcessResult> {
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

        let results: StreamRangeReply = self.client.xrevrange_count(topic, "+", "-", 1)?;

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
    ) -> RhiaqeyResult<()> {
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

        self.client.publish(&clean_topic, raw)?;
        trace!("message sent to pubsub {}", &clean_topic);

        let tag = stream_message.tag.unwrap_or(String::from(""));
        let mut items = BTreeMap::new();
        items.insert("raw", raw_message);
        items.insert("tag", tag);

        let id = self.client.xadd_maxlen_map(
            snapshot_topic.clone(),
            StreamMaxlen::Equals(channel_size),
            "*",
            items,
        )?;
        trace!("message sent to clean xstream {}: {:?}", &clean_topic, id);

        Ok(())
    }

    pub async fn start(&mut self) {
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

    pub fn get_snapshot(&mut self) -> RhiaqeyResult<Vec<StreamMessage>> {
        let keys = self.get_snapshot_keys()?;
        debug!("keys are here {:?}", keys);

        if keys.len() == 0 {
            return Ok(vec![]);
        }

        // let mut messages = Vec::new();

        let ids = vec![0; keys.len()];
        debug!("ids are key {:?}", ids);

        let options = StreamReadOptions::default().count(self.channel.size);

        let results: StreamReadReply = self.client.xread_options(&*keys, &*ids, &options)?;

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

        /*for StreamKey { key: _, ids } in results.keys {
            for StreamId { id: _, map } in ids {
                if let Some(raw) = map.get("raw") {
                    if let redis::Value::Data(ref data) = raw {
                        if let Ok(entry) = serde_json::from_slice::<StreamMessage>(data) {
                            trace!(
                                "found entry key={}, timestamp={:?}",
                                entry.key,
                                entry.timestamp
                            );
                            messages.push(entry);
                        }
                    }
                }
            }
        }*/

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

        let keys: Vec<String> = self.client.scan_match(&snapshot_topic)?.collect();
        debug!("found {} keys", keys.len());

        Ok(keys)
    }
}
