use crate::http::state::{SharedState, UpdateSettingsRequest};
use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;
use log::{debug, info};
use rhiaqey_common::pubsub::{RPCMessage, RPCMessageData};
use rhiaqey_common::topics;
use rustis::commands::{PubSubCommands, StringCommands};
use std::sync::Arc;

async fn update_settings_for_hub(payload: UpdateSettingsRequest, state: Arc<SharedState>) {
    let client = state.redis.lock().await.clone().unwrap();

    let hub_key = topics::hub_settings_key(state.get_namespace());

    debug!("saving settings for hub {}", hub_key);

    let content = serde_json::to_string(&payload.settings).unwrap();
    client.set(hub_key, content.clone()).await.unwrap();

    // stream to all hubs

    let pub_topic = topics::hub_raw_to_hub_clean_pubsub_topic(state.get_namespace());

    info!("publishing to topic {}", pub_topic);

    let rpc_message = serde_json::to_string(&RPCMessage {
        data: RPCMessageData::UpdateSettings(payload.settings),
    })
    .unwrap();
    client.publish(pub_topic, rpc_message).await.unwrap();
}

async fn update_settings_for_publishers(payload: UpdateSettingsRequest, state: Arc<SharedState>) {
    let client = state.redis.lock().await.clone().unwrap();

    let publishers_key =
        topics::publisher_settings_key(state.get_namespace(), payload.name.clone());

    debug!("saving settings for publisher {}", publishers_key);

    let content = serde_json::to_string(&payload.settings).unwrap();
    client.set(publishers_key, content.clone()).await.unwrap();

    // stream to publisher

    let pub_topic = topics::hub_to_publisher_pubsub_topic(state.get_namespace(), payload.name);

    debug!("publishing to topic {}", pub_topic);

    let rpc_message = serde_json::to_string(&RPCMessage {
        data: RPCMessageData::UpdateSettings(payload.settings),
    })
    .unwrap();
    client.publish(pub_topic, rpc_message).await.unwrap();
}

pub async fn update_settings(
    Json(payload): Json<UpdateSettingsRequest>,
    state: Arc<SharedState>,
) -> impl IntoResponse {
    info!("[POST] Update settings");

    let response = payload.settings.clone();

    if state.env.name == payload.name {
        // update hub settings
        update_settings_for_hub(payload, state).await;
    } else {
        // update publisher settings
        update_settings_for_publishers(payload, state).await;
    }

    // return response

    (StatusCode::OK, Json(response))
}
