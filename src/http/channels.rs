use crate::hub::SharedState;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use log::{debug, warn};
use rhiaqey_common::pubsub::{RPCMessage, RPCMessageType};
use rhiaqey_sdk::channel::{AssignChannelsRequest, ChannelList, CreateChannelsRequest};
use rustis::commands::{PubSubCommands, StringCommands};
use std::sync::Arc;

pub async fn create_channels(
    Json(payload): Json<CreateChannelsRequest>,
    state: Arc<SharedState>,
) -> impl IntoResponse {
    let key = format!("{}:channels", state.namespace);
    let content = serde_json::to_string(&payload).unwrap_or("{}".to_string());
    state
        .redis
        .lock()
        .await
        .as_mut()
        .unwrap()
        .set(key, content)
        .await
        .unwrap();
    (StatusCode::OK, Json(payload))
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
