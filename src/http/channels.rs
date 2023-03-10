use crate::http::state::{
    AssignChannelsRequest, CreateChannelsRequest, DeleteChannelsRequest, SharedState,
};
use crate::hub::channels::StreamingChannel;
use crate::hub::metrics::TOTAL_CHANNELS;
use axum::{http::StatusCode, response::IntoResponse, Json};
use log::{debug, info, trace, warn};
use rhiaqey_common::pubsub::{RPCMessage, RPCMessageData};
use rhiaqey_common::topics;
use rhiaqey_sdk::channel::ChannelList;
use rustis::client::BatchPreparedCommand;
use rustis::commands::{PubSubCommands, StreamCommands, StringCommands, XGroupCreateOptions};
use rustis::resp::Value;
use rustis::Result as RedisResult;
use std::sync::Arc;

pub async fn delete_channels(
    Json(payload): Json<DeleteChannelsRequest>,
    state: Arc<SharedState>,
) -> impl IntoResponse {
    info!("[Delete] Delete channels");

    let hub_channels_key = topics::hub_channels_key(state.get_namespace());
    let result: String = state
        .redis
        .lock()
        .await
        .as_mut()
        .unwrap()
        .get(hub_channels_key.clone())
        .await
        .unwrap();

    let mut channel_list: ChannelList =
        serde_json::from_str(result.as_str()).unwrap_or(ChannelList::default());

    debug!("turning off streams for channels {:?}", payload.channels);

    for streaming_channel_name in &payload.channels {
        state
            .streams
            .lock()
            .await
            .remove(streaming_channel_name.as_str());
    }

    debug!("current channel list {:?}", channel_list);

    channel_list.channels.retain_mut(|list_channel| {
        let index = payload
            .channels
            .iter()
            .position(|r| *r == list_channel.name);

        index.is_none()
    });

    debug!("updated channel list {:?}", channel_list);

    let content = serde_json::to_string(&channel_list).unwrap();

    state
        .redis
        .lock()
        .await
        .as_mut()
        .unwrap()
        .set(hub_channels_key.clone(), content)
        .await
        .unwrap();

    StatusCode::NO_CONTENT
}

pub async fn create_channels(
    Json(payload): Json<CreateChannelsRequest>,
    state: Arc<SharedState>,
) -> impl IntoResponse {
    info!("[PUT] Creating channels");

    let hub_channels_key = topics::hub_channels_key(state.get_namespace());
    let content = serde_json::to_string(&payload).unwrap_or("{}".to_string());

    let mut pipeline = state.redis.lock().await.as_mut().unwrap().create_pipeline();
    pipeline.set(hub_channels_key.clone(), content).forget();

    // 1. for every channel we create a topic that the publishers can stream to

    for channel in &payload.channels.channels {
        pipeline
            .xgroup_create(
                topics::publishers_to_hub_stream_topic(state.get_namespace(), channel.name.clone()),
                "hub",
                "$",
                XGroupCreateOptions::default().mk_stream(),
            )
            .forget();
    }

    pipeline.get::<_, ()>(hub_channels_key).queue(); // get channels back
    let pipeline_result: RedisResult<Value> = pipeline.execute().await;

    trace!("pipeline result: {:?}", pipeline_result);

    // 2. for every channel we create and store a streaming channel

    let mut total_channels = 0;
    let namespace = state.env.namespace.clone();

    for channel in &payload.channels.channels {
        let channel_name = channel.name.clone();
        if state.streams.lock().await.contains_key(&*channel_name) {
            warn!("channel {} already exists", channel_name);
            continue;
        }

        let mut streaming_channel =
            StreamingChannel::create(namespace.clone(), channel.clone()).await;

        let streaming_channel_name = streaming_channel.get_name();
        streaming_channel.setup(state.env.redis.clone()).await;

        info!(
            "starting up streaming channel {}",
            streaming_channel.channel.name
        );

        streaming_channel.start().await;

        state
            .streams
            .lock()
            .await
            .insert(streaming_channel_name, streaming_channel);

        total_channels += 1;
    }

    info!("added {} streams", total_channels);

    TOTAL_CHANNELS.set(total_channels as f64);

    (
        StatusCode::OK,
        [(hyper::header::CONTENT_TYPE, "application/json")],
        pipeline_result.unwrap().to_string(),
    )
}

pub async fn assign_channels(
    Json(payload): Json<AssignChannelsRequest>,
    state: Arc<SharedState>,
) -> impl IntoResponse {
    info!("[POST] Assign channels");

    let mut client = state.redis.lock().await.clone().unwrap();

    // get channels

    let channels_key = topics::hub_channels_key(state.get_namespace());
    let result: String = client.get(channels_key.clone()).await.unwrap();
    debug!("got channels {}", result);

    let channel_list: Result<ChannelList, _> = serde_json::from_str(result.as_str());
    if channel_list.is_err() {
        warn!(
            "error parsing channel list {}: {}",
            result,
            channel_list.unwrap_err()
        );
        return (StatusCode::BAD_REQUEST, Json(vec![]));
    }

    // populate channels with details e.g. size

    let channels = channel_list
        .unwrap()
        .channels
        .iter()
        .filter(|x| {
            payload
                .channels
                .iter()
                .find(|y| x.name.as_str() == *y)
                .is_some()
        })
        .map(|x| x.clone())
        .collect::<Vec<_>>();

    // update publisher's channels

    let publishers_key =
        topics::publisher_channels_key(state.get_namespace(), payload.name.clone());

    debug!("saving channels for publisher {}", publishers_key);

    let content = serde_json::to_string(&ChannelList {
        channels: channels.clone(),
    })
    .unwrap();
    client.set(publishers_key, content).await.unwrap();

    // stream to publisher

    let stream_topic = topics::hub_to_publisher_pubsub_topic(state.get_namespace(), payload.name);

    debug!("streaming to topic {}", stream_topic);

    let content = serde_json::to_string(&RPCMessage {
        data: RPCMessageData::AssignChannels(ChannelList {
            channels: channels.clone(),
        }),
    })
    .unwrap();
    client.publish(stream_topic, content).await.unwrap();

    // return response

    (StatusCode::OK, Json(channels))
}
