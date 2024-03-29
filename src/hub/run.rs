use crate::hub::channel::StreamingChannel;
use crate::hub::metrics::{TOTAL_CHANNELS, TOTAL_CLIENTS};
use crate::hub::Hub;
use log::{info, warn};
use rhiaqey_common::env::parse_env;

pub async fn run() {
    env_logger::init();
    let env = parse_env();
    info!(
        "running hub [id={}, name={}, namespace={}]",
        env.id, env.name, env.namespace
    );

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
