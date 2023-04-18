use log::{debug, trace, warn};
use rhiaqey_common::pubsub::{RPCMessage, RPCMessageData};
use rhiaqey_common::redis::{connect_and_ping, RedisSettings};
use rhiaqey_common::stream::StreamMessage;
use rhiaqey_common::topics;
use rhiaqey_sdk::channel::Channel;
use rustis::client::Client;
use rustis::commands::{
    PubSubCommands, StreamCommands, StreamEntry, XAddOptions, XTrimOperator, XTrimOptions,
};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct MessageHandler {
    hub_id: String,
    pub namespace: String,
    pub channel: Channel,
    pub redis: Arc<Mutex<Client>>,
}

enum MessageProcessResult {
    Allow,
    AllowUnprocessed,
    Deny(String),
    CheckIfMessageExists,
}

impl MessageProcessResult {
    pub fn should_allow(&self) -> bool {
        self == MessageProcessResult::Allow
    }

    pub fn should_allow_unprocessed(&self) -> bool {
        self == MessageProcessResult::AllowUnprocessed
    }

    pub fn should_deny(&self) -> bool {
        match self {
            MessageProcessResult::Deny(_) => true,
            _ => false,
        }
    }

    pub fn get_deny_reason(&self) -> Option<String> {
        match self {
            MessageProcessResult::Deny(reason) => Some(reason.to_string()),
            _ => None,
        }
    }

    pub fn should_check_if_message_exists(&self) -> bool {
        self == MessageProcessResult::CheckIfMessageExists
    }
}

/// Message handler per channel
impl MessageHandler {
    pub async fn create(
        hub_id: String,
        namespace: String,
        channel: Channel,
        config: RedisSettings,
    ) -> MessageHandler {
        let connection = connect_and_ping(config).await.unwrap();
        MessageHandler {
            hub_id,
            namespace,
            channel,
            redis: Arc::new(Mutex::new(connection)),
        }
    }

    async fn compare_by_timestamp(
        &mut self,
        new_message: &StreamMessage,
        topic: &String,
    ) -> MessageProcessResult {
        debug!("checking if message should be processed");

        // 1. if the new message has timestamp=0 means do not check at all.
        //    Just let it pass through.
        let new_timestamp = new_message.timestamp.unwrap_or(0);
        if new_timestamp == 0 {
            debug!("new message has timestamp = 0");
            // allow it without checking
            return MessageProcessResult::AllowUnprocessed;
        }

        let results: Vec<StreamEntry<String>> = self
            .redis
            .lock()
            .await
            .xrevrange(topic, "+", "-", Some(1))
            .await
            .unwrap_or(vec![]);

        if results.len() == 0 {
            // allow it since we have not stored data to compare against
            return MessageProcessResult::AllowUnprocessed;
        }

        // Checking only the first result
        let last_entry = results.iter().next().unwrap();
        let Some(last_message) = last_entry.items.get("raw") else {
            warn!("last message in raw not found");
            // allow it as the stored one did not have correct format
            return MessageProcessResult::AllowUnprocessed;
        };

        let decoded = StreamMessage::from_string(last_message.as_str());
        if let Err(e) = decoded {
            warn!("stored message could not be deserialized {e}");
            // allow it as the stored failed to decode
            return MessageProcessResult::AllowUnprocessed;
        }

        // =========================================================================================

        let stored_message = decoded.unwrap();
        let old_timestamp = stored_message.timestamp.unwrap_or(0);

        // 2. If old timestamp if more recent do not process the new one.
        if old_timestamp > new_timestamp {
            // do not allow it as it is not fresh data
            return MessageProcessResult::Deny(String::from("old timestamp"));
        }

        // 3. If new timestamp has the same with the stored one, then we need to check if message
        //    exists in the whole list. Must compare against all stored message and compare tags.
        if old_timestamp == new_timestamp {
            // we need to further examine the message
            return MessageProcessResult::CheckIfMessageExists;
        }

        // We are allowing anything else that does not meet out criteria to
        // deny processing the message.
        MessageProcessResult::Allow
    }

    pub async fn handle_raw_stream_message_from_publishers(
        &mut self,
        stream_message: StreamMessage,
        channel_size: usize,
    ) {
        debug!("handle raw stream message");

        let channel_size = stream_message.size.unwrap_or(channel_size) as i64;
        let mut notify_message = stream_message.clone();
        notify_message.hub_id = Some(self.hub_id.clone());

        let snapshot_topic = topics::hub_channel_snapshot_topic(
            self.namespace.clone(),
            stream_message.channel.clone(),
            stream_message.key.clone(),
            stream_message
                .category
                .clone()
                .unwrap_or(String::from("default")),
        );

        let compare_timestamps = self
            .compare_by_timestamp(&stream_message, &snapshot_topic)
            .await;

        if compare_timestamps.should_deny() {
            warn!(
                "raw message should not be processed further due to: {}",
                compare_timestamps.get_deny_reason().unwrap()
            );
            return;
        }

        if compare_timestamps.should_check_if_message_exists() {
            // TODO: check here against all messages
            trace!("must compare message against all");
        }

        if compare_timestamps.should_allow() {
            // TODO: proceed here to compare by tag
            trace!("allowing message to proceed (check by tags)");
        }

        if compare_timestamps.should_allow_unprocessed() {
            trace!("allowing message to proceed unprocessed");
        }

        let Ok(raw_message) = stream_message.to_string() else {
            warn!("failed to message to string");
            return;
        };

        let clean_topic = topics::hub_raw_to_hub_clean_pubsub_topic(self.namespace.clone());

        let raw = &RPCMessage {
            data: RPCMessageData::NotifyClients(notify_message),
        }
        .to_string()
        .unwrap();
        trace!("rpc message encoded to string {}", raw);

        self.redis
            .lock()
            .await
            .publish(clean_topic.clone(), raw)
            .await
            .unwrap();
        trace!("message sent to pubsub {}", clean_topic);

        let xadd_options = XAddOptions::default();
        let trim_options = XTrimOptions::max_len(XTrimOperator::Equal, channel_size);

        let id: String = self
            .redis
            .lock()
            .await
            .xadd(
                snapshot_topic.clone(),
                "*",
                [("raw", raw_message)],
                xadd_options.trim_options(trim_options),
            )
            .await
            .unwrap();

        debug!("message sent to clean xstream {}: {id}", clean_topic);
    }
}
