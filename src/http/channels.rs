use crate::http::common::retrieve_schema_for_hub;
use crate::http::state::{
    AssignChannelsRequest, CreateChannelsRequest, DeleteChannelsRequest, SharedState,
};
use crate::hub::simple_channel::SimpleChannels;
use anyhow::Context;
use axum::extract::{Path, Query, State};
use axum::{http::StatusCode, response::IntoResponse, Json};
use log::{debug, info, trace, warn};
use redis::Commands;
use rhiaqey_common::pubsub::{PublisherRegistrationMessage, RPCMessage, RPCMessageData};
use rhiaqey_common::stream::StreamMessage;
use rhiaqey_common::topics::{self};
use rhiaqey_sdk_rs::channel::ChannelList;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

use super::common::SnapshotParam;

pub async fn create_channels(
    payload: CreateChannelsRequest,
    state: Arc<SharedState>,
) -> anyhow::Result<String> {
    let lock = state.redis_rs.clone();
    let mut conn = lock.lock().unwrap();

    // create channels

    let hub_channels_key = topics::hub_channels_key(state.get_namespace());
    let content = serde_json::to_string(&payload).unwrap_or(String::from("{}"));
    let _: () = conn.set(hub_channels_key.clone(), content)?;

    // create xgroups

    trace!("creating xstreams");

    for channel in &payload.channels.channels {
        let _: () = conn
            .xgroup_create_mkstream(
                topics::publishers_to_hub_stream_topic(
                    state.get_namespace(),
                    channel.name.as_ref(),
                ),
                "hub",
                "$",
            )
            .unwrap_or_default();
    }

    // get result

    trace!("read back result");

    let result: String = conn.get(hub_channels_key).unwrap();
    drop(conn);

    // notify all hubs to create and start streaming channels

    state.publish_rpc_message(RPCMessageData::CreateChannels(payload.channels.channels))?;

    Ok(result)
}

pub async fn create_channels_handler(
    State(state): State<Arc<SharedState>>,
    Json(payload): Json<CreateChannelsRequest>,
) -> impl IntoResponse {
    info!("[PUT] Creating channels");

    match create_channels(payload, state).await {
        Ok(response) => (
            StatusCode::OK,
            [(hyper::header::CONTENT_TYPE, "application/json")],
            response,
        )
            .into_response(),
        Err(err) => {
            warn!("error publishing creating channels: {err}");
            return (
                StatusCode::OK,
                [(hyper::header::CONTENT_TYPE, "application/json")],
                json!({
                    "code": 500,
                    "message": "failed to create channel"
                })
                .to_string(),
            )
                .into_response();
        }
    }
}

pub async fn delete_channels_handler(
    State(state): State<Arc<SharedState>>,
    Json(payload): Json<DeleteChannelsRequest>,
) -> impl IntoResponse {
    info!("[Delete] Delete channels");

    let lock = state.redis_rs.clone();
    let mut conn = lock.lock().unwrap();

    // get channels

    let hub_channels_key = topics::hub_channels_key(state.get_namespace());
    let result: String = conn.get(hub_channels_key.clone()).unwrap();
    let mut channel_list: ChannelList = serde_json::from_str(result.as_str()).unwrap_or_default();
    trace!("channel list retrieved");

    // remove channels

    let to_be_deleted: Vec<_> = channel_list
        .channels
        .iter()
        .filter(|f| payload.channels.contains(&f.name))
        .map(|f| f.clone())
        .collect();

    channel_list.channels.retain_mut(|list_channel| {
        let index = payload
            .channels
            .iter()
            .position(|r| *r == list_channel.name);

        index.is_none()
    });
    let content = serde_json::to_string(&channel_list).unwrap_or(String::from("{}"));
    let _: () = conn.set(hub_channels_key.clone(), content).unwrap();
    drop(conn);

    // notify all hubs to stop and drop streaming channels

    match state.publish_rpc_message(RPCMessageData::DeleteChannels(to_be_deleted)) {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => {
            warn!("error publishing delete channels: {err}");
            return (
                StatusCode::OK,
                [(hyper::header::CONTENT_TYPE, "application/json")],
                json!({
                    "code": 500,
                    "message": "failed to delete channel"
                })
                .to_string(),
            )
                .into_response();
        }
    }
}

pub async fn get_publishers_handler(State(state): State<Arc<SharedState>>) -> impl IntoResponse {
    info!("[GET] Get publishers");

    let Ok(mut conn) = state.redis_rs.lock() else {
        return Json::<Vec<PublisherRegistrationMessage>>(vec![]).into_response();
    };

    // find assigned publisher keys

    let schema_key = topics::publisher_schema_key(state.get_namespace(), "*");
    debug!("schema key {}", schema_key);

    let keys: Vec<String> = conn.keys(schema_key).unwrap_or(vec![]);
    trace!("found {} keys", keys.len());

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

pub async fn get_hub_handler(State(state): State<Arc<SharedState>>) -> impl IntoResponse {
    info!("[GET] Get hub");

    let id = state.get_id();
    let name = state.get_name();
    let namespace = state.get_namespace();

    let Ok(schema) = retrieve_schema_for_hub(namespace, state.redis_rs.clone()).await else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            [(hyper::header::CONTENT_TYPE, "application/json")],
            json!({
                "code": 500,
                "message": "failed to acquire hub schema"
            })
            .to_string(),
        )
            .into_response();
    };

    trace!("schema acquired");

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
        .into_response()
}

pub async fn get_channels_handler(State(state): State<Arc<SharedState>>) -> impl IntoResponse {
    info!("[GET] Get channels");

    let lock = state.redis_rs.clone();
    let mut conn = lock.lock().unwrap();

    let channels_key = topics::hub_channels_key(state.get_namespace());
    let result: String = conn.get(channels_key).unwrap_or(String::from(""));
    let channel_list: ChannelList = serde_json::from_str(result.as_str()).unwrap_or_default();

    Json(channel_list)
}

pub async fn purge_channel_handler(
    State(state): State<Arc<SharedState>>,
    Path(channel): Path<String>,
) -> impl IntoResponse {
    info!("[DELETE] Purging channel {}", channel);

    match state.publish_rpc_message(RPCMessageData::PurgeChannels(vec![channel.clone()])) {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(err) => {
            warn!("error publishing purge channel {channel}: {err}");
            return (
                StatusCode::OK,
                [(hyper::header::CONTENT_TYPE, "application/json")],
                json!({
                    "code": 500,
                    "message": "failed to purge channel"
                })
                .to_string(),
            )
                .into_response();
        }
    }
}

pub async fn get_channel_assignments_handler(
    State(state): State<Arc<SharedState>>,
) -> impl IntoResponse {
    info!("[GET] Get channel assignments");

    let lock = state.redis_rs.clone();
    let mut conn = lock.lock().unwrap();

    // find assigned channel keys

    let publishers_key = topics::publisher_channels_key(state.get_namespace(), "*");
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
    payload: AssignChannelsRequest,
    state: Arc<SharedState>,
) -> anyhow::Result<()> {
    // get all channels in the system
    let channel_name = payload.name.clone();
    let mut conn = state.redis_rs.lock().unwrap();
    let channels_key = topics::hub_channels_key(state.get_namespace());
    let result: String = conn.get(channels_key.clone()).unwrap();
    trace!("got channels {}", result);

    // decode result into a channel list
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

    // prepare pubsub payload
    let content = serde_json::to_string(&AssignChannelsRequest {
        name: payload.name.clone(),
        channels: valid_channels
            .iter()
            .map(|x| x.name.to_string())
            .collect::<Vec<_>>(),
    })
    .unwrap();

    // store locally

    let publishers_key =
        topics::publisher_channels_key(state.get_namespace(), channel_name.as_ref());
    let _: () = conn.set(publishers_key, content).unwrap();

    // notify publishers

    let rpc_message = RPCMessage {
        data: RPCMessageData::AssignChannels(valid_channels.clone()),
    }
    .ser_to_string()
    .context("failed to serialize rpc message")
    .unwrap();

    let stream_topic =
        topics::hub_to_publisher_pubsub_topic(state.get_namespace(), channel_name.as_ref());

    let _: () = conn.publish(stream_topic, rpc_message).unwrap();

    Ok(())
}

pub async fn assign_channels_handler(
    State(state): State<Arc<SharedState>>,
    Json(payload): Json<AssignChannelsRequest>,
) -> impl IntoResponse {
    info!("[POST] Assign channels");
    debug!("[POST] Payload {:?}", payload);
    assign_channels(payload, state).await.unwrap();
    (StatusCode::NO_CONTENT,).into_response()
}

#[derive(Deserialize, Debug)]
pub struct SnapshotParams {
    channels: String,
    snapshot_size: Option<usize>,
    snapshot: Option<SnapshotParam>,
}

pub async fn get_snapshot_handler(
    State(state): State<Arc<SharedState>>,
    Query(params): Query<SnapshotParams>,
) -> impl IntoResponse {
    info!("[GET] Get snapshot: {:?}", params);

    let channels = SimpleChannels::from(params.channels.split(",").collect::<Vec<_>>());
    trace!("channel from params extracted {:?}", channels);

    if channels.is_empty() {
        return (
            StatusCode::OK,
            [(hyper::header::CONTENT_TYPE, "application/json")],
            json!({
                "code": 404,
                "message": "channels are missing"
            })
            .to_string(),
        )
            .into_response();
    }

    let mut streaming_channels = state.streams.lock().await;

    let mut result: HashMap<String, Vec<StreamMessage>> = HashMap::new();

    // With this, we will support channel names that can include categories seperated with a `/`
    // Valid examples would be `ticks` but also `ticks/historical`.
    // Any other format would be considered invalid and would be filtered out.
    let channels: Vec<(String, Option<String>, Option<String>)> =
        channels.get_channels_with_category_and_key();

    // default snapshot direction
    let direction = params.snapshot.unwrap_or_default();

    for channel in channels.iter() {
        let streaming_channel = streaming_channels.get_mut(&channel.0);
        if let Some(chx) = streaming_channel {
            let snapshot = chx
                .get_snapshot(
                    &direction,
                    channel.1.clone(),
                    channel.2.clone(),
                    params.snapshot_size,
                )
                .unwrap_or(vec![]);

            debug!(
                "found {} snapshot entries for {} channel",
                snapshot.len(),
                chx.get_channel().name
            );

            result.insert(channel.0.clone(), snapshot);
        }
    }

    (
        StatusCode::OK,
        [(hyper::header::CONTENT_TYPE, "application/json")],
        json!(result).to_string(),
    )
        .into_response()
}
