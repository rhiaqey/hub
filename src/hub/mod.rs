pub(crate) mod channel;
pub(crate) mod client;
pub(crate) mod messages;
pub(crate) mod metrics;
pub(crate) mod settings;

use crate::http::server::{start_private_http_server, start_public_http_server};
use crate::http::state::SharedState;
use crate::hub::channel::StreamingChannel;
use crate::hub::client::WebSocketClient;
use crate::hub::metrics::{TOTAL_CHANNELS, TOTAL_CLIENTS};
use crate::hub::settings::HubSettings;
use axum::extract::ws::Message;
use futures::StreamExt;
use log::{debug, info, trace, warn};
use redis::Commands;
use rhiaqey_common::client::ClientMessage;
use rhiaqey_common::env::{parse_env, Env};
use rhiaqey_common::pubsub::{PublisherRegistrationMessage, RPCMessage, RPCMessageData};
use rhiaqey_common::redis::{connect_and_ping_async, RhiaqeyBufVec};
use rhiaqey_common::redis_rs::connect_and_ping;
use rhiaqey_common::security::SecurityKey;
use rhiaqey_common::{security, topics, RhiaqeyResult};
use rhiaqey_sdk_rs::channel::{Channel, ChannelList};
use rhiaqey_sdk_rs::message::MessageValue;
use rustis::client::{Client, PubSubStream};
use rustis::commands::{PubSubCommands, StringCommands};
use sha256::digest;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Hub {
    pub env: Arc<Env>,
    pub redis: Arc<Mutex<Client>>,
    pub redis_rs: Arc<std::sync::Mutex<redis::Connection>>,
    pub security: Arc<Mutex<SecurityKey>>,
    pub settings: Arc<RwLock<HubSettings>>,
    pub clients: Arc<Mutex<HashMap<String, WebSocketClient>>>,
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

    pub fn get_public_port(&self) -> u16 {
        self.env.public_port.unwrap()
    }

    pub fn get_namespace(&self) -> String {
        self.env.namespace.clone()
    }

    pub async fn create_raw_to_hub_clean_pubsub_async(&mut self) -> RhiaqeyResult<PubSubStream> {
        let client = connect_and_ping_async(self.env.redis.clone()).await?;
        let key = topics::hub_raw_to_hub_clean_pubsub_topic(self.get_namespace());
        let stream = client.subscribe(key.clone()).await?;
        Ok(stream)
    }

    pub fn get_channels(&self) -> RhiaqeyResult<Vec<Channel>> {
        let channels_key = topics::hub_channels_key(self.get_namespace());
        let mut lock = self.redis_rs.lock().unwrap();
        let result: String = lock.get(channels_key)?;
        let channel_list: ChannelList = serde_json::from_str(result.as_str())?;
        Ok(channel_list.channels)
    }

    pub async fn set_schema_async(&self, data: PublisherRegistrationMessage) {
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
                    .set(schema_key, schema)
                    .await
                    .expect("failed to store schema in redis");

                trace!("schema saved");
            }
            Err(err) => {
                warn!("serde json error {err}");
            }
        }
    }

    pub async fn read_settings_async(&self) -> RhiaqeyResult<HubSettings> {
        let settings_key = topics::hub_settings_key(self.get_namespace());

        let result: RhiaqeyBufVec = self.redis.lock().await.get(settings_key).await?;
        debug!("encrypted settings retrieved");

        let keys = self.security.lock().await;

        let data = security::aes_decrypt(
            keys.no_once.as_slice(),
            keys.key.as_slice(),
            result.0.as_slice(),
        )?;

        let settings = MessageValue::Binary(data).decode::<HubSettings>()?;
        debug!("decrypted data decoded into settings");

        Ok(settings)
    }

    pub async fn set_settings_async(&mut self, settings: HubSettings) {
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

    async fn load_key_async(config: &Env, client: &Client) -> RhiaqeyResult<SecurityKey> {
        let namespace = config.namespace.clone();
        let security_key = topics::security_key(namespace);
        let security_str: String = client.get(security_key.clone()).await?;
        let security = match serde_json::from_str::<SecurityKey>(security_str.as_str()) {
            Ok(mut security) => {
                debug!("security keys loaded");
                security.key = config.decrypt(security.key)?;
                security.no_once = config.decrypt(security.no_once)?;
                security
            }
            Err(_) => {
                warn!("security keys not found");
                let mut security = SecurityKey::default();
                let original = security.clone();
                security.no_once = config.encrypt(security.no_once)?;
                security.key = config.encrypt(security.key)?;
                let key_result = serde_json::to_string(&security)?;
                client.set(security_key.clone(), key_result).await?;
                debug!("new keys generated and saved");
                original
            }
        };
        Ok(security)
    }

    pub async fn create(config: Env) -> RhiaqeyResult<Hub> {
        let redis_rs_client = connect_and_ping(&config.redis)?;
        let redis_rs_connection = redis_rs_client.get_connection()?;
        let client = connect_and_ping_async(config.redis.clone()).await?;
        let security = Self::load_key_async(&config, &client).await?;

        Ok(Hub {
            env: Arc::from(config),
            settings: Arc::from(RwLock::new(HubSettings::default())),
            streams: Arc::new(Mutex::new(HashMap::new())),
            redis: Arc::new(Mutex::new(client)),
            redis_rs: Arc::new(std::sync::Mutex::new(redis_rs_connection)),
            clients: Arc::new(Mutex::new(HashMap::new())),
            security: Arc::new(Mutex::new(security)),
        })
    }

    pub async fn start(&mut self) -> hyper::Result<()> {
        info!("starting hub");

        let settings = self
            .read_settings_async()
            .await
            .unwrap_or(HubSettings::default());
        self.set_settings_async(settings).await;
        debug!("settings loaded");

        let shared_state = Arc::new(SharedState {
            env: self.env.clone(),
            settings: self.settings.clone(),
            streams: self.streams.clone(),
            redis: self.redis.clone(),
            redis_rs: self.redis_rs.clone(),
            clients: self.clients.clone(),
            security: self.security.clone(),
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

        let mut clean_message_stream = self.create_raw_to_hub_clean_pubsub_async().await.unwrap();

        let hub_id = self.get_id();
        let streams = self.streams.clone();
        let clients = self.clients.clone();

        loop {
            tokio::select! {
                Some(pubsub_message) = clean_message_stream.next() => {
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
                            info!("setting publisher schema for [id={}, name={}, namespace={}]",
                                data.id, data.name, data.namespace);
                            self.set_schema_async(data).await;
                        }
                        // this comes from another hub to notify all other hubs
                        RPCMessageData::UpdateSettings() => {
                            info!("received update settings rpc");
                            match self.read_settings_async().await {
                                Ok(settings) => {
                                    self.set_settings_async(settings).await;
                                    info!("settings updated successfully");
                                },
                                Err(err) => {
                                    warn!("error decoding to hub settings: {err}")
                                }
                            }
                        }
                        // hub raw to hub clean
                        // from xstream to pubsub
                        // from a load balanced to broadcast
                        RPCMessageData::NotifyClients(stream_message) => {
                            trace!("received notify clients rpc");
                            // get a streaming channel by channel name
                            let all_hub_streams = streams.lock().await;
                            let streaming_channel = all_hub_streams.get(stream_message.channel.as_str());
                            if let Some(s_channel) = streaming_channel {
                                trace!("streaming channel found {}", s_channel.channel.name);

                                let all_stream_channel_clients = s_channel.clients.read().unwrap();

                                let mut all_hub_clients = clients.lock().await;

                                let mut client_message = ClientMessage::from(stream_message);
                                if client_message.hub_id.is_none() {
                                    client_message.hub_id = Some(hub_id.clone());
                                }

                                let raw = serde_json::to_vec(&client_message).unwrap();

                                let mut to_delete = vec!();

                                // TODO: Move broadcast to streaming channel
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
                                    trace!("message sent to {:?} client(s)", all_stream_channel_clients.len());
                                } else {
                                    warn!("disconnecting {} clients", to_delete.len());

                                    for client_id in all_stream_channel_clients.iter() {
                                        all_hub_clients.remove(client_id);
                                        let mut lock = streaming_channel.unwrap().clients.write().unwrap();
                                        if let Some(i) = lock.iter().position(|x| x == client_id) {
                                            warn!("removing client from channel by index {i}");
                                            lock.remove(i);
                                        }
                                    }

                                    let total_clients = all_hub_clients.len();
                                    TOTAL_CLIENTS.set(total_clients as f64);
                                    trace!("total clients set to {total_clients}");
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

    let mut hub = match Hub::create(env).await {
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
    let channels = hub.get_channels().unwrap_or(vec![]);
    let mut streams = hub.streams.lock().await;

    for channel in channels {
        let channel_name = channel.name.to_string();
        if streams.contains_key(&channel_name) {
            warn!("channel {} already exists", channel_name);
            continue;
        }

        let Ok(mut streaming_channel) = StreamingChannel::create(
            hub.get_id(),
            namespace.clone(),
            channel.clone(),
            hub.env.redis.clone(),
        ) else {
            warn!("failed to create streaming channel {}", channel.name);
            continue;
        };

        let streaming_channel_name = streaming_channel.get_name();

        info!(
            "starting up streaming channel {}",
            streaming_channel.channel.name
        );

        streaming_channel.start();
        streams.insert(streaming_channel_name.into(), streaming_channel);
        total_channels += 1;
    }

    info!("added {} streams", total_channels);

    drop(streams);
    TOTAL_CHANNELS.set(total_channels as f64);
    TOTAL_CLIENTS.set(0f64);

    if let Err(err) = hub.start().await {
        panic!("error starting hub: {}", err);
    }
}
