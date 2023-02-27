use crate::http::state::{SharedState, UpdateSettingsRequest};
use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;
use log::{debug, info};
use rhiaqey_common::pubsub::{RPCMessage, RPCMessageData};
use rhiaqey_common::topics;
use rustis::commands::{PubSubCommands, StringCommands};
use std::sync::Arc;

pub async fn update_settings(
    Json(payload): Json<UpdateSettingsRequest>,
    state: Arc<SharedState>,
) -> impl IntoResponse {
    info!("[POST] Update settings");

    let mut client = state.redis.lock().await.clone().unwrap();

    let publishers_key =
        topics::publisher_settings_key(state.get_namespace(), payload.name.clone());

    debug!("saving settings for publisher {}", publishers_key);

    let content = serde_json::to_string(&payload.settings).unwrap();
    client.set(publishers_key, content.clone()).await.unwrap();

    // stream to publisher

    let stream_topic = topics::hub_to_publisher_pubsub_topic(state.get_namespace(), payload.name);

    debug!("streaming to topic {}", stream_topic);

    let rpc_message = serde_json::to_string(&RPCMessage {
        data: RPCMessageData::UpdateSettings(payload.settings),
    })
    .unwrap();
    client.publish(stream_topic, rpc_message).await.unwrap();

    // return response

    (StatusCode::OK, Json(content))
}
