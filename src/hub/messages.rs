use log::{debug, trace};
use rhiaqey_common::pubsub::{RPCMessage, RPCMessageData};
use rhiaqey_common::redis::{connect_and_ping, RedisSettings};
use rhiaqey_common::stream::StreamMessage;
use rhiaqey_common::topics;
use rhiaqey_sdk::channel::Channel;
use rustis::client::Client;
use rustis::commands::{PubSubCommands, StreamCommands, XAddOptions, XTrimOperator, XTrimOptions};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct MessageHandler {
    hub_id: String,
    pub namespace: String,
    pub channel: Channel,
    pub redis: Arc<Mutex<Client>>,
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

    pub async fn handle_raw_stream_message_from_publishers(
        &mut self,
        stream_message: StreamMessage,
        channel_size: usize,
    ) {
        debug!("handle raw stream message");

        let channel_size = stream_message.size.unwrap_or(channel_size) as i64;
        let mut notify_message = stream_message.clone();
        notify_message.hub_id = Some(self.hub_id.clone());

        let raw_message = stream_message.to_string().unwrap();
        trace!("raw message encoded to string {}", raw_message);

        // TODO: handle duplicates here

        let clean_topic = topics::hub_raw_to_hub_clean_pubsub_topic(self.namespace.clone());
        let snapshot_topic = topics::hub_channel_snapshot_topic(
            self.namespace.clone(),
            stream_message.channel,
            stream_message.key,
            stream_message.category.unwrap_or(String::from("default")),
        );

        let raw = serde_json::to_string(&RPCMessage {
            data: RPCMessageData::NotifyClients(notify_message),
        })
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
