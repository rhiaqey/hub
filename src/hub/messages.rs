use log::{debug, info, warn};
use rhiaqey_common::redis::{connect_and_ping, RedisSettings};
use rhiaqey_common::stream::StreamMessage;
use rhiaqey_common::topics;
use rhiaqey_sdk::channel::Channel;
use rustis::client::Client;
use rustis::commands::{PubSubCommands, StreamCommands, StreamEntry};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct MessageHandler {
    pub namespace: String,
    pub channel: Channel,
    pub redis: Option<Arc<Mutex<Client>>>,
}

impl MessageHandler {
    pub async fn create(
        namespace: String,
        channel: Channel,
        config: RedisSettings,
    ) -> MessageHandler {
        let connection = connect_and_ping(config).await.unwrap();
        MessageHandler {
            namespace,
            channel,
            redis: Some(Arc::new(Mutex::new(connection))),
        }
    }

    pub async fn handle_raw_stream_message(&mut self, stream_message: StreamMessage) {
        info!("stream list handled {:?}", stream_message);

        let key = topics::publisher_channels_snapshot(
            self.namespace.clone(),
            stream_message.channel.clone(),
            stream_message.key.clone(),
            stream_message
                .category
                .clone()
                .unwrap_or("default".to_string()),
        );

        info!("key generated for snapshot: {}", key);

        let mut results: Result<Vec<StreamEntry<String>>, _> = self
            .redis
            .as_mut()
            .unwrap()
            .lock()
            .await
            .xrevrange(key, "+", "-", Some(1))
            .await;

        if results.is_err() {
            warn!("error with results");
            return;
        }

        let entries = results.unwrap_or(vec![]);

        if entries.len() > 0 {
            debug!("results from snapshot key {:?}", entries.len());
        }

        let raw = serde_json::to_string(&stream_message).unwrap();
        let clean_topic = topics::hub_raw_to_hub_clean_pubsub_topic(self.namespace.clone());

        self.redis
            .as_mut()
            .unwrap()
            .lock()
            .await
            .publish(clean_topic.clone(), raw)
            .await
            .unwrap();

        info!("message sent to pubsub {}", clean_topic);
    }
}
