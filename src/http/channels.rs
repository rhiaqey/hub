use crate::http::state::{
    AssignChannelsRequest, CreateChannelsRequest, DeleteChannelsRequest, SharedState,
};
use crate::hub::channel::StreamingChannel;
use crate::hub::metrics::TOTAL_CHANNELS;
use crate::hub::settings::HubSettings;
use axum::extract::{Path, Query, State};
use axum::{http::StatusCode, response::IntoResponse, Json};
use log::{debug, info, trace, warn};
use redis::Commands;
use rhiaqey_common::error::RhiaqeyError;
use rhiaqey_common::pubsub::{PublisherRegistrationMessage, RPCMessage, RPCMessageData};
use rhiaqey_common::stream::StreamMessage;
use rhiaqey_common::topics::{self};
use rhiaqey_sdk_rs::channel::ChannelList;
use rustis::client::BatchPreparedCommand;
use rustis::commands::{StreamCommands, StringCommands, XGroupCreateOptions};
use rustis::resp::Value;
use rustis::Result as RedisResult;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

pub async fn delete_channels(
    State(state): State<Arc<SharedState>>,
    Json(payload): Json<DeleteChannelsRequest>,
) -> impl IntoResponse {
    info!("[Delete] Delete channels");

    let hub_channels_key = topics::hub_channels_key(state.get_namespace());
    let result: String = state
        .redis
        .lock()
        .await
        .get(hub_channels_key.clone())
        .await
        .unwrap();

    let mut channel_list: ChannelList = serde_json::from_str(result.as_str()).unwrap_or_default();

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
        .set(hub_channels_key.clone(), content)
        .await
        .unwrap();

    StatusCode::NO_CONTENT
}

pub async fn get_publishers(State(state): State<Arc<SharedState>>) -> impl IntoResponse {
    info!("[GET] Get publishers");

    let Ok(mut conn) = state.redis_rs.lock() else {
        return Json::<Vec<PublisherRegistrationMessage>>(vec![]).into_response();
    };

    // find assigned publisher keys

    let schema_key = topics::publisher_schema_key(state.get_namespace(), "*".to_string());
    info!("schema key {}", schema_key);

    let keys: Vec<String> = conn.keys(schema_key).unwrap_or(vec![]);
    debug!("found {} keys", keys.len());

    if keys.len() == 0 {
        return Json::<Vec<PublisherRegistrationMessage>>(vec![]).into_response();
    }

    let mut pipeline = redis::pipe();
    // Dynamically add commands to the pipeline
    for key in keys.iter() {
        pipeline.add_command(redis::cmd("GET").arg(key).clone());
    }

    let result: Vec<String> = pipeline.query(&mut conn).unwrap_or(vec![]);

    let publishers: Vec<PublisherRegistrationMessage> = result
        .iter()
        .map(|x| serde_json::from_str(x))
        .filter(|z| z.is_ok())
        .map(|z| z.unwrap())
        .collect();

    Json(publishers).into_response()
}

pub async fn get_hub(State(state): State<Arc<SharedState>>) -> impl IntoResponse {
    let id = state.get_id();
    let name = state.get_name();
    let namespace = state.get_namespace();
    let schema = HubSettings::schema();

    (
        StatusCode::OK,
        [(hyper::header::CONTENT_TYPE, "application/json")],
        json!({
            "Id": id,
            "Name": name,
            "Namespace": namespace,
            "Schema": schema
        })
        .to_string(),
    )
}

pub async fn get_channels(State(state): State<Arc<SharedState>>) -> impl IntoResponse {
    info!("[GET] Get channels");

    let lock = state.redis_rs.clone();
    let mut conn = lock.lock().unwrap();

    let channels_key = topics::hub_channels_key(state.get_namespace());
    let result: String = conn.get(channels_key).unwrap_or(String::from(""));
    let channel_list: ChannelList = serde_json::from_str(result.as_str()).unwrap_or_default();

    Json(channel_list)
}

pub async fn purge_channel(
    State(state): State<Arc<SharedState>>,
    Path(channel): Path<String>,
) -> impl IntoResponse {
    info!("[DELETE] Purging channel {}", channel);

    let mut streams = state.streams.lock().await;
    let streaming_channel = streams.get_mut(&channel);
    if streaming_channel.is_none() {
        warn!(
            "could not find streaming channel by name {}",
            channel.clone()
        );
        return (
            StatusCode::NOT_FOUND,
            [(hyper::header::CONTENT_TYPE, "application/json")],
            json!({
                "message": "Streaming channel could not be found"
            })
            .to_string(),
        );
    }

    // get all keys
    let keys = streaming_channel
        .unwrap()
        .get_snapshot_keys()
        .unwrap_or(vec![]);
    debug!("{} keys found", keys.len());

    let mut conn = state.redis_rs.lock().unwrap();
    let result = conn.del(keys).unwrap_or(0);

    debug!("{result} entries purged");

    (
        StatusCode::OK,
        [(hyper::header::CONTENT_TYPE, "application/json")],
        json!({
            "count": result
        })
        .to_string(),
    )
}

pub async fn create_channels(
    State(state): State<Arc<SharedState>>,
    Json(payload): Json<CreateChannelsRequest>,
) -> impl IntoResponse {
    info!("[PUT] Creating channels");

    let hub_channels_key = topics::hub_channels_key(state.get_namespace());
    let content = serde_json::to_string(&payload).unwrap_or("{}".to_string());

    let redis = state.redis.clone().lock().await.clone();
    let mut pipeline = redis.create_pipeline();
    pipeline.set(hub_channels_key.clone(), content).forget();

    // 1. for every channel we create a topic that the publishers can stream to

    for channel in &payload.channels.channels {
        pipeline
            .xgroup_create(
                topics::publishers_to_hub_stream_topic(
                    state.get_namespace(),
                    channel.name.to_string(),
                ),
                "hub",
                "$",
                XGroupCreateOptions::default().mk_stream(),
            )
            .forget();
    }

    pipeline.get::<_, ()>(&hub_channels_key).queue(); // get channels back
    let pipeline_result: RedisResult<Value> = pipeline.execute().await;

    trace!("pipeline result: {:?}", pipeline_result);

    // 2. for every channel we create and store a streaming channel

    let mut total_channels = 0;
    let hub_id = state.env.id.clone();
    let namespace = state.env.namespace.clone();
    let mut streams = state.streams.lock().await;

    for channel in &payload.channels.channels {
        let channel_name = channel.name.to_string();
        if streams.contains_key(&channel_name) {
            warn!("channel {} already exists", channel_name);
            continue;
        }

        let Ok(mut streaming_channel) = StreamingChannel::create(
            hub_id.clone(),
            namespace.clone(),
            channel.clone(),
            state.env.redis.clone(),
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

    info!("added {} streams", total_channels);
    drop(streams);
    TOTAL_CHANNELS.set(total_channels as f64);

    (
        StatusCode::OK,
        [(hyper::header::CONTENT_TYPE, "application/json")],
        pipeline_result.unwrap().to_string(),
    )
}

pub async fn get_channel_assignments(State(state): State<Arc<SharedState>>) -> impl IntoResponse {
    info!("[GET] Get channel assignments");

    let lock = state.redis_rs.clone();
    let mut conn = lock.lock().unwrap();

    // find assigned channel keys

    let publishers_key = topics::publisher_channels_key(state.get_namespace(), "*".to_string());
    debug!("publishers key {publishers_key}");

    let keys: Vec<String> = conn.keys(publishers_key).unwrap_or(vec![]);
    debug!("found {} keys", keys.len());

    if keys.len() == 0 {
        return Json::<Vec<AssignChannelsRequest>>(vec![]).into_response();
    }

    let mut pipeline = redis::pipe();
    // Dynamically add commands to the pipeline
    for key in keys.iter() {
        pipeline.add_command(redis::cmd("GET").arg(key).clone());
    }

    let result: Vec<String> = pipeline.query(&mut conn).unwrap_or(vec![]);

    let assignments: Vec<AssignChannelsRequest> = result
        .iter()
        .map(|x| serde_json::from_str(x))
        .filter(|z| z.is_ok())
        .map(|z| z.unwrap())
        .collect();

    Json(assignments).into_response()
}

pub async fn assign_channels(
    State(state): State<Arc<SharedState>>,
    Json(payload): Json<AssignChannelsRequest>,
) -> impl IntoResponse {
    info!("[POST] Assign channels");
    debug!("[POST] Payload {:?}", payload);

    let channel_name = payload.name;

    let mut conn = state.redis_rs.lock().unwrap();
    trace!("client lock acquired");

    // calculate channels key
    let channels_key = topics::hub_channels_key(state.get_namespace());
    trace!("channels key {}", channels_key);

    // get all channels in the system
    let result: String = conn.get(channels_key.clone()).unwrap();
    trace!("got channels {}", result);

    let all_channels: ChannelList = serde_json::from_str(result.as_str()).unwrap_or_default();

    // find only valid channels
    let valid_channels = all_channels
        .channels
        .iter()
        .filter(|x| {
            // extract only the valid ones
            payload
                .channels
                .iter()
                .find(|y| x.name == y.as_str())
                .is_some()
        })
        .map(|x| x.clone())
        .collect::<Vec<_>>();

    let publishers_key =
        topics::publisher_channels_key(state.get_namespace(), channel_name.clone());

    debug!("saving channels for publisher {}", publishers_key);

    let assigned_channels = valid_channels
        .iter()
        .map(|x| x.name.to_string())
        .collect::<Vec<_>>();

    let content = serde_json::to_string(&AssignChannelsRequest {
        name: channel_name.clone(),
        channels: assigned_channels,
    })
    .unwrap();

    let _: () = conn.set(publishers_key, content).unwrap();

    // stream to publisher

    let stream_topic =
        topics::hub_to_publisher_pubsub_topic(state.get_namespace(), channel_name.clone());

    debug!("streaming to topic {}", stream_topic);

    let content = serde_json::to_string(&RPCMessage {
        data: RPCMessageData::AssignChannels(ChannelList {
            channels: valid_channels.clone(),
        }),
    })
    .unwrap();
    let _: () = conn.publish(stream_topic, content).unwrap();

    // return response

    (StatusCode::OK, Json(valid_channels))
}

#[derive(Deserialize)]
pub struct SnapshotParams {
    channels: String,
}

pub async fn get_snapshot(
    State(state): State<Arc<SharedState>>,
    Query(params): Query<SnapshotParams>,
) -> impl IntoResponse {
    info!("[GET] Get snapshot");

    let channels: Vec<String> = params.channels.split(",").map(|x| x.to_string()).collect();
    trace!("channel from params extracted {:?}", channels);

    if channels.is_empty() {
        return RhiaqeyError::from("channels are missing").into_response();
    }

    let mut streaming_channels = state.streams.lock().await;

    let mut result: HashMap<String, Vec<StreamMessage>> = HashMap::new();

    for channel in channels.iter() {
        let streaming_channel = streaming_channels.get_mut(channel);
        if let Some(chx) = streaming_channel {
            result.insert(channel.clone(), chx.get_snapshot().unwrap_or(vec![]));
        }
    }

    (
        StatusCode::OK,
        [(hyper::header::CONTENT_TYPE, "application/json")],
        json!(result).to_string(),
    )
        .into_response()
}
