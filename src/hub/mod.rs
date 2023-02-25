pub mod channels;
pub mod messages;

use crate::http::server::{start_private_http_server, start_public_http_server};
use crate::http::state::SharedState;
use crate::hub::channels::StreamingChannel;
use axum::extract::ws::{Message, WebSocket};
use futures::StreamExt;
use log::{debug, info, trace, warn};
use rhiaqey_common::client::{ClientMessage, ClientMessageDataType, ClientMessageValue};
use rhiaqey_common::env::{parse_env, Env};
use rhiaqey_common::pubsub::{RPCMessage, RPCMessageData};
use rhiaqey_common::redis::connect_and_ping;
use rhiaqey_common::{redis, topics};
use rhiaqey_sdk::channel::{Channel, ChannelList};
use rustis::client::{Client, PubSubStream};
use rustis::commands::{ConnectionCommands, PingOptions, PubSubCommands, StringCommands};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Clone)]
pub struct Hub {
    pub env: Arc<Env>,
    pub redis: Arc<Mutex<Option<Client>>>,
    pub streams: Arc<Mutex<HashMap<String, StreamingChannel>>>,
    pub clients: Arc<Mutex<HashMap<Uuid, WebSocket>>>,
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

    pub fn get_public_port(&self) -> u16 {
        self.env.public_port.unwrap()
    }

    pub async fn create_raw_to_hub_clean_pubsub(&mut self) -> Option<PubSubStream> {
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

        Ok(Hub {
            env: Arc::from(config),
            streams: Arc::new(Mutex::new(HashMap::new())),
            redis: Arc::new(Mutex::new(redis_connection)),
            clients: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn start(&mut self) -> hyper::Result<()> {
        let shared_state = Arc::new(SharedState {
            env: self.env.clone(),
            streams: self.streams.clone(),
            redis: self.redis.clone(),
            clients: self.clients.clone(),
        });

        let private_port = self.get_private_port();
        let private_state = Arc::clone(&shared_state);

        tokio::spawn(async move { start_private_http_server(private_port, private_state).await });

        let public_port = self.get_public_port();
        let public_state = Arc::clone(&shared_state);

        tokio::spawn(
            async move { start_public_http_server(public_port, public_state.clone()).await },
        );

        let mut pubsub_stream = self.create_raw_to_hub_clean_pubsub().await.unwrap();

        let streams = self.streams.clone();
        let clients = self.clients.clone();

        loop {
            tokio::select! {
                Some(pubsub_message) = pubsub_stream.next() => {
                    trace!("clean message arrived");
                    if pubsub_message.is_err() {
                        warn!("invalid clean message");
                        continue;
                    }

                    let data = serde_json::from_slice::<RPCMessage>(pubsub_message.unwrap().payload.as_slice());
                    if data.is_err() {
                        warn!("failed to parse rpc message");
                        continue;
                    }

                    info!("clean message arrived");

                    match data.unwrap().data {
                        RPCMessageData::NotifyClients(stream_message) => {
                            debug!("stream message arrived {:?}", stream_message);

                            // get streaming channel by channel name
                            let all_streams = streams.lock().await;
                            let streaming_channel = all_streams.get(&stream_message.channel);
                            if streaming_channel.is_some() {
                                debug!("streaming channel found");

                                let streaming_clients = streaming_channel.unwrap().clients.lock().await;
                                let mut all_clients = clients.lock().await;

                                let client_message = ClientMessage {
                                    data_type: ClientMessageDataType::Data as u8,
                                    channel: stream_message.channel,
                                    key: stream_message.key,
                                    value: ClientMessageValue::Data(stream_message.value),
                                    tag: stream_message.tag,
                                    category: stream_message.category,
                                    hub_id: stream_message.hub_id,
                                    publisher_id: stream_message.publisher_id
                                };

                                let raw = serde_json::to_vec(&client_message).unwrap();

                                for client in streaming_clients.as_slice() {
                                    trace!("must notify client {:?}", client);
                                    match all_clients.get_mut(client) {
                                        Some(socket) => {
                                            match socket.send(Message::Binary(raw.clone())).await {
                                                Ok(_) => debug!("message sent"),
                                                Err(e) => warn!("failed to sent message: {e}")
                                            }
                                        },
                                        None => {}
                                    }
                                }

                                info!("message sent to {:?} client(s)", streaming_clients.len());
                            }
                        }
                        _ => {}
                    }
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

    for channel in channels {
        let channel_name = channel.name.clone();
        if hub.streams.lock().await.contains_key(&*channel_name) {
            warn!("channel {} already exists", channel_name);
            continue;
        }

        let mut streaming_channel =
            StreamingChannel::create(namespace.clone(), channel.clone()).await;

        let streaming_channel_name = streaming_channel.get_name();
        streaming_channel.setup(hub.env.redis.clone()).await;

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

    hub.start().await.expect("[hub]: Failed to start");
}
