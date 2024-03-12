use crate::http::state::{
    AssignChannelsRequest, CreateChannelsRequest, DeleteChannelsRequest, SharedState,
};
use crate::hub::channels::StreamingChannel;
use crate::hub::metrics::TOTAL_CHANNELS;
use crate::hub::settings::HubSettings;
use axum::extract::{Path, State};
use axum::{http::StatusCode, response::IntoResponse, Json};
use log::{debug, info, trace, warn};
use rhiaqey_common::pubsub::{PublisherRegistrationMessage, RPCMessage, RPCMessageData};
use rhiaqey_common::topics::{self};
use rhiaqey_sdk_rs::channel::ChannelList;
use rustis::client::BatchPreparedCommand;
use rustis::commands::{
    GenericCommands, PubSubCommands, StreamCommands, StringCommands, XGroupCreateOptions,
};
use rustis::resp::Value;
use rustis::Result as RedisResult;
use serde_json::json;
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

pub async fn get_publishers(State(state): State<Arc<SharedState>>) -> impl IntoResponse {
    info!("[GET] Get publishers");

    let client = state.redis.clone().lock().await.clone().unwrap();

    // find assigned publisher keys

    let schema_key = topics::publisher_schema_key(state.get_namespace(), "*".to_string());
    info!("schema key {}", schema_key);

    let keys: Vec<String> = client.keys(schema_key).await.unwrap_or(vec![]);
    debug!("found {} keys", keys.len());

    if keys.len() == 0 {
        return Json::<Vec<PublisherRegistrationMessage>>(vec![]).into_response();
    }

    let mut pipeline = client.create_pipeline();
    keys.iter().for_each(|x| {
        pipeline.get::<_, ()>(x).queue(); // get channels back
    });

    match pipeline.execute::<Value>().await {
        Ok(result) => {
            debug!("retrieved results");

            (
                StatusCode::OK,
                [(hyper::header::CONTENT_TYPE, "application/json")],
                result.to_string(),
            )
                .into_response()
        }
        Err(err) => {
            warn!("there is an error with pipeline execute {err}");
            Json::<Vec<PublisherRegistrationMessage>>(vec![]).into_response()
        }
    }
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

pub async fn get_channels(State(state): State<Arc<SharedState>>) -> Json<ChannelList> {
    info!("[GET] Get channels");

    let hub_channels_key = topics::hub_channels_key(state.get_namespace());
    info!("channels key {}", hub_channels_key);

    let client = state.redis.clone().lock().await.clone().unwrap();

    match client.get::<_, String>(hub_channels_key).await {
        Ok(channels) => {
            if let Ok(channel_list) = serde_json::from_str::<ChannelList>(channels.as_str()) {
                debug!("{} channels found", channel_list.channels.len());
                return Json(channel_list);
            }
        }
        Err(err) => {
            warn!("error fetching channels {}", err);
        }
    };

    Json(ChannelList::default())
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
    let keys = streaming_channel.unwrap().get_snapshot_keys().await;
    debug!("{} keys found", keys.len());

    let redis = state.redis.clone().lock().await.clone().unwrap();
    let result = redis.del(keys).await.unwrap_or(0);

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

    let redis = state.redis.clone().lock().await.clone().unwrap();
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

        let mut streaming_channel =
            StreamingChannel::create(hub_id.clone(), namespace.clone(), channel.clone()).await;

        if let Err(err) = streaming_channel.setup(state.env.redis.clone()).await {
            warn!("error stream setup for channel {} - {}", channel_name, err);
            continue;
        }

        info!(
            "starting up streaming channel {}",
            streaming_channel.channel.name
        );

        streaming_channel.start().await;
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

    let client = state.redis.clone().lock().await.clone().unwrap();

    // find assigned channel keys

    let publishers_key = topics::publisher_channels_key(state.get_namespace(), "*".to_string());
    debug!("publishers key {publishers_key}");

    let keys: Vec<String> = client.keys(publishers_key).await.unwrap_or(vec![]);
    debug!("found {} keys", keys.len());

    if keys.len() == 0 {
        return Json::<Vec<AssignChannelsRequest>>(vec![]).into_response();
    }

    let mut pipeline = client.create_pipeline();
    keys.iter().for_each(|x| {
        pipeline.get::<_, ()>(x).queue(); // get channels back
    });

    match pipeline.execute::<Value>().await {
        Ok(result) => {
            debug!("retrieved results");

            (
                StatusCode::OK,
                [(hyper::header::CONTENT_TYPE, "application/json")],
                result.to_string(),
            )
                .into_response()
        }
        Err(err) => {
            warn!("there is an error with pipeline execute {err}");
            Json::<Vec<AssignChannelsRequest>>(vec![]).into_response()
        }
    }
}

pub async fn assign_channels(
    State(state): State<Arc<SharedState>>,
    Json(payload): Json<AssignChannelsRequest>,
) -> impl IntoResponse {
    info!("[POST] Assign channels");
    debug!("[POST] Payload {:?}", payload);

    let channel_name = payload.name;

    let client = state.redis.lock().await.clone().unwrap();
    trace!("client lock acquired");

    // calculate channels key
    let channels_key = topics::hub_channels_key(state.get_namespace());
    trace!("channels key {}", channels_key);

    // get all channels in the system
    let result: String = client.get(channels_key.clone()).await.unwrap();
    trace!("got channels {}", result);

    let all_channels: ChannelList =
        serde_json::from_str(result.as_str()).unwrap_or(ChannelList::default());

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

    client.set(publishers_key, content).await.unwrap();

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
    client.publish(stream_topic, content).await.unwrap();

    // return response

    (StatusCode::OK, Json(valid_channels))
}
