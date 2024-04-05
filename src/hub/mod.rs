pub mod channel;
pub(crate) mod client;
pub(crate) mod messages;
pub mod metrics;
pub mod run;
pub(crate) mod settings;

use crate::http::server::{start_private_http_server, start_public_http_server};
use crate::http::state::SharedState;
use crate::hub::channel::StreamingChannel;
use crate::hub::client::WebSocketClient;
use crate::hub::metrics::TOTAL_CHANNELS;
use crate::hub::settings::HubSettings;
use axum::extract::ws::Message;
use futures::stream::select_all;
use futures::StreamExt;
use log::{debug, info, trace, warn};
use redis::Commands;
use rhiaqey_common::client::ClientMessage;
use rhiaqey_common::env::Env;
use rhiaqey_common::pubsub::{PublisherRegistrationMessage, RPCMessage, RPCMessageData};
use rhiaqey_common::redis_rs::connect_and_ping;
use rhiaqey_common::security::SecurityKey;
use rhiaqey_common::stream::StreamMessage;
use rhiaqey_common::{result::RhiaqeyResult, security, topics};
use rhiaqey_sdk_rs::channel::{Channel, ChannelList};
use rhiaqey_sdk_rs::message::MessageValue;
use sha256::digest;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Hub {
    pub env: Arc<Env>,
    pub redis_rs: Arc<std::sync::Mutex<redis::Connection>>,
    pub security: Arc<RwLock<SecurityKey>>,
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

    pub fn get_channels(&self) -> RhiaqeyResult<Vec<Channel>> {
        let channels_key = topics::hub_channels_key(self.get_namespace());
        let mut lock = self.redis_rs.lock().unwrap();
        let result: String = lock.get(channels_key)?;
        let channel_list: ChannelList = serde_json::from_str(result.as_str())?;
        Ok(channel_list.channels)
    }

    pub fn read_settings(&self) -> RhiaqeyResult<HubSettings> {
        info!("reading hub settings");

        let settings_key = topics::hub_settings_key(self.get_namespace());
        let result: Vec<u8> = self.redis_rs.lock().unwrap().get(settings_key)?;
        trace!("encrypted settings retrieved");

        let keys = self.security.read().unwrap();

        let data = security::aes_decrypt(
            keys.no_once.as_slice(),
            keys.key.as_slice(),
            result.as_slice(),
        )?;
        trace!("settings decrypted");

        let settings = MessageValue::Binary(data).decode::<HubSettings>()?;
        trace!("decrypted data decoded into settings");

        Ok(settings)
    }

    pub fn set_settings(&mut self, settings: HubSettings) {
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

        *locked_settings = new_settings.clone();

        trace!("new settings updated");
    }

    fn load_key(config: &Env, client: &mut redis::Connection) -> RhiaqeyResult<SecurityKey> {
        let namespace = config.namespace.clone();
        let security_key = topics::security_key(namespace);
        let security_str: String = client.get(security_key.clone()).unwrap_or(String::from(""));
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
                client.set(security_key.clone(), key_result)?;
                debug!("new keys generated and saved");
                original
            }
        };
        Ok(security)
    }

    pub async fn create(config: Env) -> RhiaqeyResult<Hub> {
        let redis_rs_client = connect_and_ping(&config.redis)?;
        let mut redis_rs_connection = redis_rs_client.get_connection()?;
        let security = Self::load_key(&config, &mut redis_rs_connection)?;

        Ok(Hub {
            env: Arc::from(config),
            settings: Arc::from(RwLock::new(HubSettings::default())),
            streams: Arc::new(Mutex::new(HashMap::new())),
            redis_rs: Arc::new(std::sync::Mutex::new(redis_rs_connection)),
            clients: Arc::new(Mutex::new(HashMap::new())),
            security: Arc::new(RwLock::new(security)),
        })
    }

    pub async fn start(&mut self) -> RhiaqeyResult<()> {
        info!("starting hub");

        let settings = self.read_settings().unwrap_or(HubSettings::default());
        self.set_settings(settings);
        debug!("settings loaded");

        let namespace = self.env.namespace.clone();

        let shared_state = Arc::new(SharedState {
            env: self.env.clone(),
            settings: self.settings.clone(),
            streams: self.streams.clone(),
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

        let client = connect_and_ping(&self.env.redis.clone()).unwrap();
        let mut pubsub = client.get_async_pubsub().await.unwrap();
        let key = topics::hub_raw_to_hub_clean_pubsub_topic(namespace);
        pubsub.subscribe(key).await.unwrap();
        let pubsub_stream = pubsub.on_message();
        let mut streamz = select_all(vec![Box::pin(pubsub_stream)]);

        while let Some(pubsub_message) = streamz.next().await {
            trace!("clean message arrived");

            let Ok(msg_str) = pubsub_message.get_payload::<String>() else {
                warn!("invalid clean message");
                continue;
            };

            let data = serde_json::from_str::<RPCMessage>(msg_str.as_str());
            if data.is_err() {
                warn!("failed to parse rpc message");
                continue;
            }

            self.handle_rpc_message(data.unwrap()).await;
        }

        Ok(())
    }

    async fn handle_rpc_message(&mut self, data: RPCMessage) {
        match data.data {
            RPCMessageData::PurgeChannels(channels) => {
                info!("purging {} channels", channels.len());
                for channel in channels.iter() {
                    self.purge_channel(channel)
                        .await
                        .expect(format!("failed to purge channel {channel}").as_str());
                }
            }
            RPCMessageData::CreateChannels(channels) => {
                info!("creating channels {:?}", channels);
                self.create_channels(self.get_id(), channels)
                    .await
                    .expect("failed to create channels")
            }
            RPCMessageData::DeleteChannels(channels) => {
                info!("deleting channels {:?}", channels);
                self.delete_channels(channels)
                    .await
                    .expect("failed to delete channels");
            }
            RPCMessageData::RegisterPublisher(data) => {
                info!(
                    "setting publisher schema for [id={}, name={}, namespace={}]",
                    data.id, data.name, data.namespace
                );
                self.update_publisher_schema(data)
                    .expect("failed to set schema for publisher");
            }
            RPCMessageData::UpdateHubSettings() => {
                info!("received update settings rpc");
                self.update_hub_settings()
                    .expect("failed to update hub settings");
            }
            RPCMessageData::NotifyClients(stream_message) => {
                info!("received notify clients rpc");
                self.notify_clients(self.get_id(), stream_message)
                    .await
                    .expect("failed to notify clients");
            }
            other => {
                warn!("handler for rpc message missing: {:?}", other);
            }
        };
    }

    async fn create_channels(&self, hub_id: String, channels: Vec<Channel>) -> RhiaqeyResult<()> {
        let mut total_channels = 0;
        let namespace = self.get_namespace();
        let mut streams = self.streams.lock().await;

        for channel in channels.iter() {
            let channel_name = channel.name.to_string();
            if streams.contains_key(&channel_name) {
                warn!("channel {} already exists", channel_name);
                continue;
            }

            let Ok(mut streaming_channel) = StreamingChannel::create(
                hub_id.clone(),
                namespace.clone(),
                channel.clone(),
                self.env.redis.clone(),
            ) else {
                warn!("failed to create streaming channel {}", channel.name);
                continue;
            };

            info!(
                "starting up streaming channel {}",
                streaming_channel.channel.name
            );

            streaming_channel.start();
            streams.insert(streaming_channel.get_name(), streaming_channel);
            total_channels += 1;
        }

        drop(streams);
        info!("added {} streams", total_channels);
        TOTAL_CHANNELS.set(total_channels as f64);

        Ok(())
    }

    async fn delete_channels(&self, channels: Vec<Channel>) -> RhiaqeyResult<()> {
        let total_channels = channels.len();
        let mut lock = self.streams.lock().await;

        for channel in channels {
            lock.remove(&channel.name.to_string());
        }

        debug!("{} channels deleted", total_channels);

        Ok(())
    }

    async fn notify_clients(&self, hub_id: String, message: StreamMessage) -> RhiaqeyResult<()> {
        // get a streaming channel by channel name
        let category = message.category.clone();
        let all_hub_streams = self.streams.lock().await;
        let streaming_channel = all_hub_streams.get(message.channel.as_str());

        if let Some(s_channel) = streaming_channel {
            let channel_name = s_channel.channel.name.clone();
            trace!("streaming channel found {}", channel_name);

            let all_stream_channel_clients = s_channel.clients.read().unwrap();

            let mut all_hub_clients = self.clients.lock().await;

            let mut client_message = ClientMessage::from(message);
            if client_message.hub_id.is_none() {
                client_message.hub_id = Some(hub_id.clone());
            }

            let raw = serde_json::to_vec(&client_message).unwrap();

            let mut total = 0u32;

            for client_id in all_stream_channel_clients.iter() {
                match all_hub_clients.get_mut(client_id) {
                    Some(client) => {
                        if let Some(cat) = client.get_category_for_channel(&channel_name) {
                            if !category.eq(&Some(cat)) {
                                warn!("skipping message broadcast as the categories do not match: {:?}", category);
                                continue;
                            }
                        }

                        if let Err(e) = client.send(Message::Binary(raw.clone())).await {
                            warn!("failed to sent message: {e}")
                        } else {
                            trace!("message sent successfully to client {client_id}");
                            total += 1;
                        }
                    }
                    None => warn!("failed to find client by id {client_id}"),
                }
            }

            info!(
                "notified {}/{}/{} clients",
                total,
                s_channel.get_total_clients(),
                all_hub_clients.len()
            );

            Ok(())
        } else {
            Err("could not find a streaming channel".to_string().into())
        }
    }

    fn update_publisher_schema(&self, data: PublisherRegistrationMessage) -> RhiaqeyResult<()> {
        let schema_key = topics::publisher_schema_key(data.namespace.clone(), data.name.clone());
        let encoded = serde_json::to_string(&data)?;
        let lock = self.redis_rs.clone();
        lock.lock().unwrap().set(schema_key, encoded)?;
        debug!("schema updated");
        Ok(())
    }

    fn update_hub_settings(&mut self) -> RhiaqeyResult<()> {
        match self.read_settings() {
            Ok(settings) => {
                self.set_settings(settings);
                info!("settings updated successfully");
                Ok(())
            }
            Err(err) => {
                warn!("error decoding to hub settings: {err}");
                Err(err)
            }
        }
    }

    async fn purge_channel(&self, channel: &String) -> RhiaqeyResult<()> {
        debug!("purging channel {channel}");

        let mut streams = self.streams.lock().await;
        match streams.get_mut(channel) {
            None => {
                warn!("could not find streaming channel by name {}", channel);
                Err(format!("could not find streaming channel by name {}", channel).into())
            }
            Some(streaming_channel) => {
                trace!("streaming channel found");

                let keys = streaming_channel.delete_snapshot_keys().unwrap_or(0);
                trace!("{keys} snapshot keys deleted");

                let entries = streaming_channel.xtrim().unwrap_or(0);
                trace!("{entries} entries xtrimmed");

                Ok(())
            }
        }
    }
}
