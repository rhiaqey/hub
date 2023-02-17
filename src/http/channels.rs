use crate::http::SharedState;
use crate::hub::channels::StreamingChannel;
use axum::{http::StatusCode, response::IntoResponse, Json};
use log::{debug, trace, warn};
use rhiaqey_common::pubsub::{RPCMessage, RPCMessageData};
use rhiaqey_common::topics;
use rhiaqey_sdk::channel::{AssignChannelsRequest, ChannelList, CreateChannelsRequest};
use rustis::client::BatchPreparedCommand;
use rustis::commands::{PubSubCommands, StreamCommands, StringCommands, XGroupCreateOptions};
use rustis::resp::Value;
use rustis::Result as RedisResult;
use std::sync::Arc;

pub async fn create_channels(
    Json(payload): Json<CreateChannelsRequest>,
    state: Arc<SharedState>,
) -> impl IntoResponse {
    let hub_channels_key = topics::hub_channels_key(state.namespace.clone());
    let content = serde_json::to_string(&payload).unwrap_or("{}".to_string());

    let mut pipeline = state.redis.lock().await.as_mut().unwrap().create_pipeline();
    pipeline.set(hub_channels_key.clone(), content).forget();

    // 1. for every channel we create a topic that the publishers can stream to

    for channel in &payload.channels.channels {
        pipeline
            .xgroup_create(
                topics::publishers_to_hub_stream_topic(
                    state.namespace.clone(),
                    channel.name.clone(),
                ),
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

    for channel in &payload.channels.channels {
        let streaming_channel = StreamingChannel::new(channel.clone());
        streaming_channel.start();
        state
            .streams
            .write()
            .await
            .insert(streaming_channel.get_name(), streaming_channel);
    }

    debug!("added {} streams", state.streams.read().await.len());

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
    let mut client = state.redis.lock().await.clone().unwrap();

    // get channels

    let channels_key = topics::hub_channels_key(state.namespace.clone());
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
        topics::publisher_channels_key(state.namespace.clone(), payload.name.clone());

    debug!("saving channels for publisher {}", publishers_key);

    let content = serde_json::to_string(&ChannelList {
        channels: channels.clone(),
    })
    .unwrap();
    client.set(publishers_key, content).await.unwrap();

    // stream to publisher

    let stream_topic = topics::hub_to_publisher_pubsub_topic(state.namespace.clone(), payload.name);

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
