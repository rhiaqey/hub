pub mod channels;
pub mod messages;

use crate::http::server::start_http_server;
use crate::http::state::SharedState;
use crate::hub::channels::StreamingChannel;
use futures::StreamExt;
use log::{debug, info, trace, warn};
use rhiaqey_common::env::{parse_env, Env};
use rhiaqey_common::redis::connect_and_ping;
use rhiaqey_common::{redis, topics};
use rhiaqey_sdk::channel::{Channel, ChannelList};
use rustis::client::{Client, PubSubStream};
use rustis::commands::{ConnectionCommands, PingOptions, PubSubCommands, StringCommands};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Hub {
    pub env: Arc<Env>,
    pub sender: Arc<Mutex<UnboundedSender<u128>>>,
    pub receiver: Arc<Mutex<UnboundedReceiver<u128>>>,
    pub redis: Arc<Mutex<Option<Client>>>,
    pub streams: Arc<Mutex<HashMap<String, StreamingChannel>>>,
}

impl Hub {
    pub fn get_id(&self) -> String {
        self.env.id.clone()
    }

    pub fn get_name(&self) -> String {
        self.env.name.clone()
    }

    pub fn get_private_port(&self) -> u16 {
        self.env.private_port.unwrap()
    }

    pub async fn create_hub_to_publishers_pubsub(&mut self) -> Option<PubSubStream> {
        let client = connect_and_ping(self.env.redis.clone()).await;
        if client.is_none() {
            warn!("failed to connect with ping");
            return None;
        }

        let key = topics::hub_raw_to_hub_clean_pubsub_topic(self.env.namespace.clone());

        let stream = client.unwrap().subscribe(key.clone()).await.unwrap();

        Some(stream)
    }

    pub async fn get_channels(&self) -> Vec<Channel> {
        let channels_key = topics::hub_channels_key(self.env.namespace.clone());

        let result: String = self
            .redis
            .lock()
            .await
            .as_mut()
            .unwrap()
            .get(channels_key.clone())
            .await
            .unwrap();

        let channel_list: ChannelList =
            serde_json::from_str(result.as_str()).unwrap_or(ChannelList::default());

        debug!(
            "channels from {} retrieved {:?}",
            channels_key, channel_list
        );

        channel_list.channels
    }

    pub async fn setup(config: Env) -> Result<Hub, String> {
        let redis_connection = redis::connect(config.redis.clone()).await;
        let result: String = redis_connection
            .clone()
            .unwrap()
            .ping(PingOptions::default().message("hello"))
            .await
            .unwrap();
        if result != "hello" {
            return Err("ping failed".to_string());
        }

        let (sender, receiver) = unbounded_channel::<u128>();

        Ok(Hub {
            env: Arc::from(config),
            sender: Arc::new(Mutex::new(sender)),
            receiver: Arc::new(Mutex::new(receiver)),
            streams: Arc::new(Mutex::new(HashMap::new())),
            redis: Arc::new(Mutex::new(redis_connection)),
        })
    }

    pub async fn start(&self) -> hyper::Result<()> {
        let port = self.get_private_port();

        let sender = self.sender.lock().await.clone();
        let mut receiver = self.receiver.lock().await;

        let shared_state = Arc::new(SharedState {
            env: self.env.clone(),
            streams: self.streams.clone(),
            redis: self.redis.clone(),
            sender,
        });

        tokio::spawn(async move { start_http_server(port, shared_state).await });

        loop {
            tokio::select! {
                Some(message) = receiver.recv() => {
                    trace!("message received from channel: {:?}", message);
                }
            }
        }
    }
}

pub async fn run() {
    env_logger::init();
    let env = parse_env();
    let namespace = env.namespace.clone();

    let mut hub = match Hub::setup(env).await {
        Ok(exec) => exec,
        Err(error) => {
            panic!("failed to setup hub: {error}");
        }
    };

    info!(
        "hub [id={}, name={}] is ready",
        hub.get_id(),
        hub.get_name()
    );

    let mut total_channels = 0;
    let channels = hub.get_channels().await;
    let sender = hub.sender.lock().await.clone();

    for channel in channels {
        let channel_name = channel.name.clone();
        if hub.streams.lock().await.contains_key(&*channel_name) {
            warn!("channel {} already exists", channel_name);
            continue;
        }

        let mut streaming_channel =
            StreamingChannel::create(namespace.clone(), channel.clone()).await;

        let streaming_channel_name = streaming_channel.get_name();
        streaming_channel
            .setup(sender.clone(), hub.env.redis.clone())
            .await;

        info!(
            "starting up streaming channel {}",
            streaming_channel.channel.name
        );

        streaming_channel.start().await;

        hub.streams
            .lock()
            .await
            .insert(streaming_channel_name, streaming_channel);

        total_channels += 1;
    }

    info!("added {} streams", total_channels);

    let mut pubsub_stream = hub.create_hub_to_publishers_pubsub().await.unwrap();

    tokio::spawn(async move {
        hub.start().await.expect("[hub]: Failed to start");
    });

    loop {
        tokio::select! {
            Some(pubsub_message) = pubsub_stream.next() => {
                debug!("clean message arrived {:?}", pubsub_message);
            }
        }
    }
}
