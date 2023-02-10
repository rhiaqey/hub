use crate::hub::SharedState;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use rhiaqey_sdk::channel::{AssignChannelsRequest, ChannelList, CreateChannelsRequest};
use rustis::commands::{PubSubCommands, StringCommands};
use std::sync::Arc;

// create channels
// assign channels to producer/gateway
// delete channels
// basic handler that responds with a static string

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
    let channel_list: ChannelList = serde_json::from_str(result.as_str()).unwrap();
    let channels = channel_list
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
    let stream = format!("{}:{}:pubsub", state.namespace, payload.name);
    let content = serde_json::to_string(&channels).unwrap_or("{}".to_string());

    client.set(key, content.clone()).await.unwrap();
    client.publish(stream, content.clone());

    (StatusCode::OK, Json(channels))
}
