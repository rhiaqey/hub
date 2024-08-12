use crate::http::server::{start_private_http_server, start_public_http_server};
use crate::http::state::SharedState;
use crate::hub::sse_client::SSEClient;
use crate::hub::websocket_client::WebSocketClient;
use crate::hub::metrics::TOTAL_CHANNELS;
use crate::hub::settings::HubSettings;
use crate::hub::simple_channel::SimpleChannels;
use crate::hub::streaming_channel::StreamingChannel;
use anyhow::{bail, Context};
use futures::stream::select_all;
use futures::StreamExt;
use log::{debug, info, trace, warn};
use redis::Commands;
use rhiaqey_common::env::Env;
use rhiaqey_common::pubsub::{PublisherRegistrationMessage, RPCMessage, RPCMessageData};
use rhiaqey_common::redis_rs::connect_and_ping;
use rhiaqey_common::security::SecurityKey;
use rhiaqey_common::stream::StreamMessage;
use rhiaqey_common::{security, topics};
use rhiaqey_sdk_rs::channel::{Channel, ChannelList};
use rhiaqey_sdk_rs::message::MessageValue;
use sha256::digest;
use tokio::sync::broadcast::{Receiver, Sender};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::signal;
use tokio::sync::{broadcast, Mutex};

pub struct Hub {
    pub(crate) env: Arc<Env>,
    redis_rs: Arc<std::sync::Mutex<redis::Connection>>,
    security: Arc<RwLock<SecurityKey>>,
    settings: Arc<RwLock<HubSettings>>,
    sse_sender: Arc<Mutex<Sender<String>>>,
    sse_receiver: Arc<Mutex<Receiver<String>>>,
    sse_clients: Arc<Mutex<HashMap<String, SSEClient>>>,
    websocket_clients: Arc<Mutex<HashMap<String, WebSocketClient>>>,
    pub(crate) streams: Arc<Mutex<HashMap<String, StreamingChannel>>>,
}

impl Hub {
    #[inline]
    pub fn get_id(&self) -> &str {
        self.env.get_id()
    }

    #[inline]
    pub fn get_name(&self) -> &str {
        self.env.get_name()
    }

    #[inline]
    pub fn get_namespace(&self) -> &str {
        self.env.get_namespace()
    }

    #[inline]
    pub fn get_private_port(&self) -> u16 {
        self.env.get_private_port()
    }

    #[inline]
    pub fn get_public_port(&self) -> u16 {
        self.env.get_public_port()
    }

    pub fn get_channels(&self) -> anyhow::Result<Vec<Channel>> {
        let channels_key = topics::hub_channels_key(self.get_namespace());
        let mut lock = self.redis_rs.lock().unwrap();
        let result: String = lock
            .get(channels_key)
            .context("failed to retrieve channels")?;
        let channel_list: ChannelList =
            serde_json::from_str(result.as_str()).context("failed to deserialize channels")?;
        Ok(channel_list.channels)
    }

    pub fn retrieve_settings(&self) -> anyhow::Result<Vec<u8>> {
        trace!("retrieve settings");

        let settings_key = topics::hub_settings_key(self.get_namespace());
        let result: Vec<u8> = self
            .redis_rs
            .lock()
            .unwrap()
            .get(settings_key)
            .context("failed to acquire lock")?;

        trace!("encrypted settings retrieved");

        let keys = self.security.read().unwrap();

        let data = security::aes_decrypt(
            keys.no_once.as_slice(),
            keys.key.as_slice(),
            result.as_slice(),
        )
        .context("failed to decrypt settings with key")?;

        trace!("settings decrypted");

        Ok(data)
    }

    pub fn read_settings(&self) -> anyhow::Result<HubSettings> {
        info!("reading hub settings");

        let data = self.retrieve_settings()?;

        let settings = MessageValue::Binary(data)
            .decode::<HubSettings>()
            .context("failed to decode settings")?;

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

    fn load_key(config: &Env, client: &mut redis::Connection) -> anyhow::Result<SecurityKey> {
        let namespace = config.get_namespace();
        let security_key = topics::security_key(namespace);
        let security_str = client.get(security_key.clone()).unwrap_or(String::from(""));

        let security = match serde_json::from_str::<SecurityKey>(security_str.as_str()) {
            Ok(mut security) => {
                info!("security keys loaded");

                security.key = config
                    .decrypt(security.key)
                    .context("failed to decrypt security key")?;
                debug!("security key decrypted");

                security.no_once = config
                    .decrypt(security.no_once)
                    .context("failed to decrypt no once")?;
                debug!("no_once decrypted");

                security
            }
            Err(_) => {
                warn!("security keys not found");
                let mut security = SecurityKey::default();
                let original = security.clone();

                security.no_once = config
                    .encrypt(security.no_once)
                    .context("failed to encrypt no once")?;
                debug!("no_once encrypted");

                security.key = config
                    .encrypt(security.key)
                    .context("failed to encrypt security key")?;
                debug!("security key encrypted");

                let key_result = serde_json::to_string(&security).context("failed to serialize")?;
                client
                    .set(security_key.clone(), key_result)
                    .context("failed to store security key")?;

                info!("new keys generated and saved");

                original
            }
        };
        Ok(security)
    }

    pub fn create(config: Env) -> anyhow::Result<Self> {
        let redis_rs_client = connect_and_ping(&config.redis)?;

        let mut redis_rs_connection = redis_rs_client
            .get_connection()
            .context("failed to get redis connection")?;

        let security = Self::load_key(&config, &mut redis_rs_connection)
            .context("failed to load security key")?;

        let (tx, rx) = broadcast::channel::<String>(1);

        Ok(Self {
            env: Arc::from(config),
            settings: Arc::from(RwLock::new(HubSettings::default())),
            streams: Arc::new(Mutex::new(HashMap::new())),
            redis_rs: Arc::new(std::sync::Mutex::new(redis_rs_connection)),
            sse_sender: Arc::new(Mutex::new(tx)),
            sse_receiver: Arc::new(Mutex::new(rx)),
            sse_clients: Arc::new(Mutex::new(HashMap::new())),
            websocket_clients: Arc::new(Mutex::new(HashMap::new())),
            security: Arc::new(RwLock::new(security)),
        })
    }

    pub fn create_shared_state(&self) -> Arc<SharedState> {
        Arc::new(SharedState {
            env: self.env.clone(),
            #[cfg(not(debug_assertions))]
            settings: self.settings.clone(),
            streams: self.streams.clone(),
            redis_rs: self.redis_rs.clone(),
            sse_sender: self.sse_sender.clone(),
            sse_receiver: self.sse_receiver.clone(),
            sse_clients: self.sse_clients.clone(),
            websocket_clients: self.websocket_clients.clone(),
            security: self.security.clone(),
        })
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        info!("starting hub");

        let settings = self.read_settings().unwrap_or(HubSettings::default());
        self.set_settings(settings);
        debug!("settings loaded");

        let namespace = self.env.get_namespace();
        let private_port = self.get_private_port();
        let shared_state = self.create_shared_state();
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

        loop {
            tokio::select! {
                Ok(result) = signal::ctrl_c() => {
                    trace!("signal caught: {:?}", result);
                    break;
                },
                Some(pubsub_message) = streamz.next() => {
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
            }
        }

        info!("shutting down");

        Ok(())
    }

    async fn handle_rpc_message(&mut self, data: RPCMessage) {
        match data.data {
            RPCMessageData::PurgeChannels(channels) => {
                debug!("purging {} channels", channels.len());

                let channels: Vec<(String, Option<String>, Option<String>)> =
                    SimpleChannels::from(channels).get_channels_with_category_and_key();

                for channel in channels.iter() {
                    self.purge_channel(channel.0.clone(), channel.1.clone())
                        .await
                        .expect(format!("failed to purge channel {}", channel.0).as_str());
                }
            }
            RPCMessageData::CreateChannels(channels) => {
                debug!("creating channels {:?}", channels);
                self.create_channels(channels)
                    .await
                    .expect("failed to create channels")
            }
            RPCMessageData::DeleteChannels(channels) => {
                debug!("deleting channels {:?}", channels);
                self.delete_channels(channels)
                    .await
                    .expect("failed to delete channels");
            }
            RPCMessageData::RegisterPublisher(data) => {
                debug!(
                    "setting publisher schema for [id={}, name={}, namespace={}]",
                    data.id, data.name, data.namespace
                );
                self.update_publisher_schema(data)
                    .expect("failed to set schema for publisher");
            }
            RPCMessageData::UpdateHubSettings() => {
                debug!("received update settings rpc");
                self.update_hub_settings()
                    .expect("failed to update hub settings");
            }
            RPCMessageData::NotifyClients(stream_message) => {
                debug!("received notify clients rpc");
                self.notify_clients(stream_message)
                    .await
                    .expect("failed to notify clients");
            }
            other => {
                warn!("handler for rpc message missing: {:?}", other);
            }
        };
    }

    async fn create_channels(&self, channels: Vec<Channel>) -> anyhow::Result<()> {
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
                self.get_id().to_string(),
                namespace.to_string(),
                channel.clone(),
                self.env.redis.clone(),
            ) else {
                warn!("failed to create streaming channel {}", channel.name);
                continue;
            };

            info!(
                "starting up streaming channel {}",
                streaming_channel.get_channel().name
            );

            streaming_channel.start();
            streams.insert(
                streaming_channel.get_channel().name.clone(),
                streaming_channel,
            );
            total_channels += 1;
        }

        info!("added {} streams", total_channels);
        drop(streams);

        let channels_len = self.get_channels().unwrap_or_default().len();
        info!("total channels {}", channels_len);
        TOTAL_CHANNELS.set(channels_len as i64);

        Ok(())
    }

    async fn delete_channels(&self, channels: Vec<Channel>) -> anyhow::Result<()> {
        let total_channels = channels.len();
        let mut lock = self.streams.lock().await;

        for channel in channels {
            lock.remove(&channel.name.to_string());
        }

        debug!("{} channels deleted", total_channels);

        let channels_len = self.get_channels().unwrap_or_default().len();
        info!("total channels {}", channels_len);
        TOTAL_CHANNELS.set(channels_len as i64);

        Ok(())
    }

    async fn notify_clients(&self, message: StreamMessage) -> anyhow::Result<()> {
        let mut all_hub_streams = self.streams.lock().await;
        let streaming_channel = all_hub_streams.get_mut(message.channel.as_str());

        let Some(s_channel) = streaming_channel else {
            bail!(
                "could not find a streaming channel by name: {}",
                message.channel
            )
        };

        s_channel.broadcast_to_websocket_clients(message, self.websocket_clients.clone()).await
    }

    fn update_publisher_schema(&self, data: PublisherRegistrationMessage) -> anyhow::Result<()> {
        let schema_key: String =
            topics::publisher_schema_key(data.namespace.as_str(), data.name.as_str());
        let encoded = serde_json::to_string(&data)?;
        let lock = self.redis_rs.clone();
        lock.lock()
            .unwrap()
            .set(schema_key, encoded)
            .context("failed to store schema")?;
        debug!(
            "schema updated for {}[id={}]",
            data.name.clone(),
            data.id.clone()
        );
        Ok(())
    }

    fn update_hub_settings(&mut self) -> anyhow::Result<()> {
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

    async fn purge_channel(&self, channel: String, category: Option<String>) -> anyhow::Result<()> {
        debug!("purging channel {channel}");

        let mut streams = self.streams.lock().await;
        match streams.get_mut(&channel) {
            None => {
                warn!("could not find streaming channel by name {}", channel);
                bail!(format!(
                    "could not find streaming channel by name {}",
                    channel
                ))
            }
            Some(streaming_channel) => {
                trace!("streaming channel found");

                let keys = streaming_channel
                    .delete_snapshot_keys(category, None)
                    .unwrap_or(0);
                trace!("{keys} snapshot keys deleted");

                let entries = streaming_channel.xtrim().unwrap_or(0);
                trace!("{entries} entries xtrimmed");

                Ok(())
            }
        }
    }
}
