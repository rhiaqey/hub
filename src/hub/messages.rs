use anyhow::{bail, Context};
use log::{trace, warn};
use redis::streams::{StreamMaxlen, StreamRangeReply};
use redis::Commands;
use rhiaqey_common::pubsub::{RPCMessage, RPCMessageData};
use rhiaqey_common::stream::StreamMessage;
use rhiaqey_common::topics;
use rhiaqey_sdk_rs::channel::Channel;
use std::collections::BTreeMap;

#[derive(PartialEq)]
enum MessageProcessResult {
    Allow(StreamMessage),
    AllowUnprocessed,
    Deny(String),
    CheckIfMessageExists,
}

pub struct MessageHandler {
    hub_id: String,
    namespace: String,
    channel: Channel,
    redis_rs: redis::Connection,
}

/// Message handler per channel
impl MessageHandler {
    pub fn create(
        hub_id: String,
        channel: Channel,
        namespace: String,
        redis_rs_connection: redis::Connection,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            hub_id,
            channel,
            namespace,
            redis_rs: redis_rs_connection,
        })
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
        &mut self,
        new_message: &StreamMessage,
        topic: &String,
    ) -> anyhow::Result<MessageProcessResult> {
        trace!("checking if message should be processed 1-to-many (compare by tags)");
        trace!("checking topic {}", topic);

        let new_message = new_message.clone();
        let new_tag = new_message.tag.unwrap_or(String::from(""));

        let results: StreamRangeReply =
            self.redis_rs
                .xrevrange_count(topic, "+", "-", self.channel.size)?;

        if results.ids.len() == 0 {
            trace!("allow it unprocessed since we have not stored data to compare against");
            return Ok(MessageProcessResult::AllowUnprocessed);
        }

        trace!("found {} result ids", results.ids.len());

        // Checking all results
        for entry in results.ids.iter() {
            if let Some(stored_tag) = entry.map.get("tag") {
                if let redis::Value::BulkString(old_tag) = stored_tag {
                    if let Ok(old_tag_str) = String::from_utf8(old_tag.clone()) {
                        if new_tag.eq(&old_tag_str) {
                            trace!("tag \"{new_tag}\" already found stored");
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
        new_message: &StreamMessage,
        topic: &String,
    ) -> anyhow::Result<MessageProcessResult> {
        trace!("checking if message should be processed (compare by timestamps)");
        trace!("checking topic {}", topic);

        // 1. if the new message has timestamp=0 means do not check at all.
        //    Let it pass through.
        let new_timestamp = new_message.timestamp.unwrap_or(0);
        if new_timestamp == 0 {
            trace!("new message has timestamp = 0");
            // allow it without checking
            return Ok(MessageProcessResult::AllowUnprocessed);
        }

        let results: StreamRangeReply = self.redis_rs.xrevrange_count(topic, "+", "-", 1)?;

        if results.ids.len() == 0 {
            // allow it since we have not stored data to compare against
            return Ok(MessageProcessResult::AllowUnprocessed);
        }

        trace!("found {}", results.ids.len());

        // Checking only the first result
        let last_entry = results.ids.iter().next().unwrap();

        let Some(last_message) = last_entry.map.get("raw") else {
            trace!("last message in raw not found");
            // allow it as the stored one did not have a correct format
            return Ok(MessageProcessResult::AllowUnprocessed);
        };

        let redis::Value::BulkString(msg) = last_message else {
            bail!("could not extract bytes from last message")
        };

        let x = String::from_utf8(msg.clone())?;
        let decoded = StreamMessage::der_from_string(x.as_str())?;

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
        stream_message: &StreamMessage,
    ) -> anyhow::Result<()> {
        trace!("handle raw stream message");

        let tag = stream_message.tag.clone().unwrap_or(String::from(""));
        let channel_size = stream_message.size.unwrap_or(self.channel.size);
        let mut new_message = stream_message.clone();
        new_message.hub_id = Some(self.hub_id.clone());

        let snapshot_topic = topics::hub_channel_snapshot_topic(
            self.namespace.clone(),
            stream_message.channel.clone().into(),
            stream_message
                .category
                .clone()
                .unwrap_or(String::from("default")),
            stream_message.key.clone(),
        );

        let compare_timestamps = self.compare_by_timestamp(stream_message, &snapshot_topic)?;

        if let MessageProcessResult::Deny(reason) = compare_timestamps {
            trace!("raw message should not be processed further due to: {reason}");
            return Ok(());
        }

        if let MessageProcessResult::CheckIfMessageExists = compare_timestamps {
            trace!("must compare message against all");

            let compare_against_all = self.compare_against_all(&new_message, &snapshot_topic)?;

            if let MessageProcessResult::Deny(reason) = compare_against_all {
                trace!("raw message should not be processed further due to: {reason}");
                return Ok(());
            }

            trace!("message tags are not same 1-to-many");
        }

        if let MessageProcessResult::Allow(ref old_message) = compare_timestamps {
            trace!("allowing message to proceed (check by tags)");

            let compare_tags = Self::compare_by_tags(&new_message, old_message);

            if let MessageProcessResult::Deny(reason) = compare_tags {
                trace!("raw message should not be processed further due to: {reason}");
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
        let rpc_message = RPCMessage {
            data: RPCMessageData::NotifyClients(new_message),
        }
        .ser_to_string()
        .context("failed to serialize rpc message")?;

        let _: () = self.redis_rs.publish(&clean_topic, rpc_message)?;
        trace!("message sent to pubsub {}", &clean_topic);

        let mut items = BTreeMap::new();
        items.insert("raw", raw_message);
        items.insert("tag", tag);

        let id: String = self.redis_rs.xadd_maxlen_map(
            snapshot_topic.clone(),
            StreamMaxlen::Equals(channel_size),
            "*",
            items,
        )?;

        trace!("message sent to clean xstream {}: {:?}", &clean_topic, id);

        Ok(())
    }
}
