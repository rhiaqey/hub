use crate::http::SharedState;
use axum::{http::StatusCode, response::IntoResponse, Json};
use log::{debug, trace, warn};
use rhiaqey_common::pubsub::{RPCMessage, RPCMessageType};
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
    let key = format!("{}:channels", state.namespace);
    let content = serde_json::to_string(&payload).unwrap_or("{}".to_string());
    let mut pipeline = state.redis.lock().await.as_mut().unwrap().create_pipeline();
    pipeline.set(key.clone(), content).forget();
    for channel in &payload.channels.channels {
        let topic = format!("{}:{}:streams:raw", state.namespace, channel.name);
        pipeline
            .xgroup_create(
                topic,
                "hub",
                "$",
                XGroupCreateOptions::default().mk_stream(),
            )
            .forget();
    }
    pipeline.get::<_, ()>(key).queue();
    let pipeline_result: RedisResult<Value> = pipeline.execute().await;
    trace!("pipeline result: {:?}", pipeline_result);
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
    let key = format!("{}:channels", state.namespace);
    let result: String = client.get(key.clone()).await.unwrap();
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

    let key = format!("{}:{}:channels", state.namespace, payload.name);
    let stream = format!(
        "{}:{}:streams:pubsub",
        state.namespace, payload.name /* publisher */
    );

    debug!("publishing to {}", stream);

    let content = serde_json::to_string(&ChannelList {
        channels: channels.clone(),
    })
    .unwrap();
    client.set(key, content).await.unwrap();

    debug!("streaming to {}", stream);

    let content = serde_json::to_string(&RPCMessage {
        data: RPCMessageType::AssignChannels(ChannelList {
            channels: channels.clone(),
        }),
    })
    .unwrap();
    client.publish(stream, content).await.unwrap();

    (StatusCode::OK, Json(channels))
}
