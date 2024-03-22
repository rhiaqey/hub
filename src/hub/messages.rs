use log::{debug, trace, warn};
use redis::streams::{StreamMaxlen, StreamRangeReply};
use redis::Commands;
use rhiaqey_common::pubsub::{RPCMessage, RPCMessageData};
use rhiaqey_common::stream::StreamMessage;
use rhiaqey_common::{topics, RhiaqeyResult};
use rhiaqey_sdk_rs::channel::Channel;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(PartialEq)]
enum MessageProcessResult {
    Allow(StreamMessage),
    AllowUnprocessed,
    Deny(String),
    CheckIfMessageExists,
}

pub struct MessageHandler {
    pub hub_id: String,
    pub namespace: String,
    pub channel: Channel,
    pub redis: Arc<std::sync::Mutex<redis::Connection>>,
}

/// Message handler per channel
impl MessageHandler {
    pub fn create(
        hub_id: String,
        channel: Channel,
        namespace: String,
        redis_rs_connection: Arc<std::sync::Mutex<redis::Connection>>,
    ) -> Self {
        MessageHandler {
            hub_id,
            channel,
            namespace,
            redis: redis_rs_connection,
        }
    }

    fn compare_by_tags(new_msg: &StreamMessage, old_msg: &StreamMessage) -> MessageProcessResult {
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
    ) -> RhiaqeyResult<MessageProcessResult> {
        trace!("checking if message should be processed 1-to-many (compare by tags)");
        trace!("checking topic {}", topic);

        let lock = self.redis.clone();
        let mut client = lock.try_lock().unwrap();

        let new_message = new_message.clone();
        let new_tag = new_message.tag.unwrap_or("".to_string());

        let results: StreamRangeReply =
            client.xrevrange_count(topic, "+", "-", self.channel.size)?;

        if results.ids.len() == 0 {
            // allow it since we have not stored data to compare against
            return Ok(MessageProcessResult::AllowUnprocessed);
        }

        trace!("found {}", results.ids.len());

        // Checking all results
        for entry in results.ids.iter() {
            if let Some(stored_tag) = entry.map.get("tag") {
                if let redis::Value::Data(bytes) = stored_tag {
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
        &self,
        new_message: &StreamMessage,
        topic: &String,
    ) -> RhiaqeyResult<MessageProcessResult> {
        trace!("checking if message should be processed (compare by timestamps)");
        trace!("checking topic {}", topic);

        let lock = self.redis.clone();
        let mut client = lock.try_lock().unwrap();

        // 1. if the new message has timestamp=0 means do not check at all.
        //    Let it pass through.
        let new_timestamp = new_message.timestamp.unwrap_or(0);
        if new_timestamp == 0 {
            debug!("new message has timestamp = 0");
            // allow it without checking
            return Ok(MessageProcessResult::AllowUnprocessed);
        }

        let results: StreamRangeReply = client.xrevrange_count(topic, "+", "-", 1)?;

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

        let redis::Value::Data(bytes) = last_message else {
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
        //    exists in the whole list.
        //    Must compare against all stored messages and compare tags.
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

    // Handle all raw messages coming unfiltered from all publishers
    pub fn handle_stream_message_from_publishers(
        &mut self,
        stream_message: StreamMessage,
    ) -> RhiaqeyResult<()> {
        debug!("handle raw stream message");

        let channel_size = stream_message.size.unwrap_or(self.channel.size);
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

        let compare_timestamps = self.compare_by_timestamp(&stream_message, &snapshot_topic)?;

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

            let compare_tags = Self::compare_by_tags(&new_message, old_message);

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

        let mut client = self.redis.lock().unwrap();
        client.publish(&clean_topic, raw)?;
        trace!("message sent to pubsub {}", &clean_topic);

        let tag = stream_message.tag.unwrap_or(String::from(""));
        let mut items = BTreeMap::new();
        items.insert("raw", raw_message);
        items.insert("tag", tag);

        let id = client.xadd_maxlen_map(
            snapshot_topic.clone(),
            StreamMaxlen::Equals(channel_size),
            "*",
            items,
        )?;
        trace!("message sent to clean xstream {}: {:?}", &clean_topic, id);

        Ok(())
    }
}
