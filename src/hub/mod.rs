pub(crate) mod channels;
pub(crate) mod messages;
pub(crate) mod metrics;
pub(crate) mod settings;

use crate::http::client::WebSocketClient;
use crate::http::server::{start_private_http_server, start_public_http_server};
use crate::http::state::SharedState;
use crate::hub::channels::StreamingChannel;
use crate::hub::metrics::{TOTAL_CHANNELS, TOTAL_CLIENTS};
use crate::hub::settings::HubSettings;
use axum::extract::ws::Message;
use futures::StreamExt;
use log::{debug, info, trace, warn};
use rhiaqey_common::client::ClientMessage;
use rhiaqey_common::env::{parse_env, Env};
use rhiaqey_common::error::RhiaqeyError;
use rhiaqey_common::pubsub::{PublisherRegistrationMessage, RPCMessage, RPCMessageData};
use rhiaqey_common::redis::{connect_and_ping, RhiaqeyBufVec};
use rhiaqey_common::{redis, topics};
use rhiaqey_sdk_rs::channel::{Channel, ChannelList};
use rhiaqey_sdk_rs::message::MessageValue;
use rustis::client::{Client, PubSubStream};
use rustis::commands::{ConnectionCommands, PingOptions, PubSubCommands, StringCommands};
use sha256::digest;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Hub {
    pub env: Arc<Env>,
    pub settings: Arc<RwLock<HubSettings>>,
    pub redis: Arc<Mutex<Option<Client>>>,
    pub streams: Arc<Mutex<HashMap<Cow<'static, str>, StreamingChannel>>>,
    pub clients: Arc<Mutex<HashMap<Cow<'static, str>, WebSocketClient>>>,
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

    pub fn get_namespace(&self) -> String {
        self.env.namespace.clone()
    }

    pub async fn create_raw_to_hub_clean_pubsub(&mut self) -> Option<PubSubStream> {
        let client = connect_and_ping(self.env.redis.clone()).await;
        if client.is_none() {
            warn!("failed to connect with ping");
            return None;
        }

        let key = topics::hub_raw_to_hub_clean_pubsub_topic(self.get_namespace());

        let stream = client.unwrap().subscribe(key.clone()).await.unwrap();

        Some(stream)
    }

    pub async fn get_channels(&self) -> Vec<Channel> {
        let channels_key = topics::hub_channels_key(self.get_namespace());

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

    pub async fn set_schema(&self, data: PublisherRegistrationMessage) {
        let msg = data.clone();

        let name = data.name;
        let namespace = data.namespace;
        let schema_key = topics::publisher_schema_key(namespace, name.clone());
        debug!("schema key {schema_key}");

        match serde_json::to_string(&msg) {
            Ok(schema) => {
                let id = data.id;
                debug!("schema arrived for {}:{}", id, name);

                self.redis
                    .lock()
                    .await
                    .as_mut()
                    .expect("failed to acquire redis lock")
                    .set(schema_key, schema)
                    .await
                    .expect("failed to store in redis");

                trace!("schema saved");
            }
            Err(err) => {
                warn!("serde json error {err}");
            }
        }
    }

    pub async fn read_settings(&self) -> Result<HubSettings, RhiaqeyError> {
        let settings_key = topics::hub_settings_key(self.get_namespace());

        let result: RhiaqeyBufVec = self
            .redis
            .lock()
            .await
            .as_mut()
            .unwrap()
            .get(settings_key)
            .await?;
        debug!("encrypted settings retrieved");

        let data = self.env.decrypt(result.0)?;
        debug!("raw data decrypted");

        let settings = MessageValue::Binary(data).decode::<HubSettings>()?;
        debug!("decrypted data decoded into settings");

        Ok(settings)
    }

    pub async fn set_settings(&mut self, settings: HubSettings) {
        let mut locked_settings = self.settings.write().unwrap();

        let mut new_settings = settings.clone();
        new_settings.security.api_keys = new_settings
            .security
            .api_keys
            .iter_mut()
            .map(|x| {
                let mut y = x.clone();
                y.api_key = digest(y.api_key.clone());
                y
            })
            .collect();

        *locked_settings = new_settings;

        trace!("new settings updated");
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
            settings: Arc::from(RwLock::new(HubSettings::default())),
            streams: Arc::new(Mutex::new(HashMap::new())),
            redis: Arc::new(Mutex::new(redis_connection)),
            clients: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn start(&mut self) -> hyper::Result<()> {
        info!("starting hub");

        self.set_settings(self.read_settings().await.unwrap_or(HubSettings::default()))
            .await;
        debug!("setting set successfully");

        let shared_state = Arc::new(SharedState {
            env: self.env.clone(),
            settings: self.settings.clone(),
            streams: self.streams.clone(),
            redis: self.redis.clone(),
            clients: self.clients.clone(),
        });

        let private_port = self.get_private_port();
        let private_state = Arc::clone(&shared_state);

        tokio::spawn(async move {
            start_private_http_server(private_port, private_state).await;
        });

        let public_port = self.get_public_port();
        let public_state = Arc::clone(&shared_state);

        tokio::spawn(async move {
            start_public_http_server(public_port, public_state.clone()).await;
        });

        let mut pubsub_stream = self.create_raw_to_hub_clean_pubsub().await.unwrap();

        let hub_id = self.get_id();
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

                    match data.unwrap().data {
                        RPCMessageData::RegisterPublisher(data) => {
                            info!("setting publisher schema");
                            self.set_schema(data).await;
                        }
                        // this comes from other hub to notify all other hubs
                        RPCMessageData::UpdateSettings() => {
                            debug!("received update settings rpc");
                            match self.read_settings().await {
                                Ok(settings) => {
                                    self.set_settings(settings).await;
                                    info!("settings updated successfully");
                                },
                                Err(err) => {
                                    warn!("error decoding to hub settings: {err}")
                                }
                            }
                        }
                        // hub raw to hub clean
                        // from xstream to pubsub
                        // from load balanced to broadcast
                        RPCMessageData::NotifyClients(stream_message) => {
                            debug!("received notify clients rpc");
                            // get streaming channel by channel name
                            let all_hub_streams = streams.lock().await;
                            let streaming_channel = all_hub_streams.get(stream_message.channel.as_str());
                            if streaming_channel.is_some() {
                                debug!("streaming channel found");

                                let lock = streaming_channel.unwrap().clients.lock().await;
                                let all_stream_channel_clients = lock.to_vec();
                                drop(lock);

                                let mut all_hub_clients = clients.lock().await;

                                let mut client_message = ClientMessage::from(stream_message);
                                if client_message.hub_id.is_none() {
                                    client_message.hub_id = Some(hub_id.clone());
                                }

                                let raw = serde_json::to_vec(&client_message).unwrap();

                                let mut to_delete = vec!();

                                for client_id in all_stream_channel_clients.iter() {
                                    trace!("must notify client {:?}", client_id);

                                    match all_hub_clients.get_mut(client_id) {
                                        Some(socket) => {
                                            match socket.send(Message::Binary(raw.clone())).await {
                                                Ok(_) => trace!("message sent to {client_id}"),
                                                Err(e) => {
                                                    warn!("failed to sent message: {e}");
                                                    to_delete.push(client_id);
                                                }
                                            }
                                        },
                                        None => {
                                            warn!("failed to find client by id {client_id}");
                                        }
                                    }
                                }

                                if to_delete.is_empty() {
                                    info!("message sent to {:?} client(s)", all_stream_channel_clients.len());
                                } else {
                                    warn!("disconnecting {} clients", to_delete.len());

                                    for client_id in all_stream_channel_clients.iter() {
                                        all_hub_clients.remove(client_id);
                                        let mut lock = streaming_channel.unwrap().clients.lock().await;
                                        if let Some(i) = lock.iter().position(|x| x == client_id) {
                                            warn!("removing client from channel by index {i}");
                                            lock.remove(i);
                                        }
                                    }

                                    let total_clients = all_hub_clients.len();
                                    TOTAL_CLIENTS.set(total_clients as f64);
                                    info!("total clients set to {total_clients}");
                                }
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
        Err(err) => {
            panic!("failed to setup hub: {}", err);
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
            StreamingChannel::create(hub.get_id(), namespace.clone(), channel.clone()).await;

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
            .insert(streaming_channel_name.into(), streaming_channel);

        total_channels += 1;
    }

    info!("added {} streams", total_channels);

    TOTAL_CHANNELS.set(total_channels as f64);
    TOTAL_CLIENTS.set(0f64);

    hub.start().await.expect("[hub]: Failed to start");
}
