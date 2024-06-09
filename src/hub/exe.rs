use crate::hub::hub::Hub;
use crate::hub::metrics::{TOTAL_CHANNELS, TOTAL_CLIENTS};
use crate::hub::streaming_channel::StreamingChannel;
use log::{info, warn};
use rhiaqey_common::env::parse_env;

pub async fn create() -> Hub {
    env_logger::init();
    let env = parse_env();

    info!(
        "running hub [id={}, name={}, namespace={}]",
        env.get_id(),
        env.get_name(),
        env.get_namespace()
    );

    let namespace = env.get_namespace();

    let hub = match Hub::create(env) {
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

        let streaming_channel_name = streaming_channel.get_channel().name.clone();
        info!("starting up streaming channel {}", streaming_channel_name);
        streaming_channel.start();
        streams.insert(streaming_channel_name.into(), streaming_channel);
        total_channels += 1;
    }

    info!("added {} streams", total_channels);

    drop(streams);
    TOTAL_CHANNELS.set(total_channels as f64);
    TOTAL_CLIENTS.set(0f64);

    hub
}

pub async fn run() {
    let mut hub = create().await;

    if let Err(err) = hub.start().await {
        panic!("error starting hub: {}", err);
    }

    info!("shutting down");
}
