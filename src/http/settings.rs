use crate::http::state::{SharedState, UpdateSettingsRequest};
use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;
use log::{info, warn};
use rhiaqey_common::error::RhiaqeyError;
use rhiaqey_common::pubsub::{RPCMessage, RPCMessageData};
use rhiaqey_common::topics;
use rhiaqey_sdk_rs::message::MessageValue;
use rustis::commands::{PubSubCommands, StringCommands};
use std::sync::Arc;

async fn update_settings_for_hub(
    payload: UpdateSettingsRequest,
    state: Arc<SharedState>,
) -> Result<MessageValue, String> {
    let client = state.redis.lock().await.clone().unwrap();

    // 1. encrypt settings

    let setting = state
        .env
        .encrypt(payload.settings.to_vec().unwrap())
        .map_err(|x| x.message)?;

    // 2. save encrypted in redis

    let hub_settings_key = topics::hub_settings_key(state.get_namespace());
    client
        .set(hub_settings_key.clone(), setting)
        .await
        .map_err(|x| x.to_string())?;

    // 3. notify all other hubs

    let hub_pub_topic = topics::hub_raw_to_hub_clean_pubsub_topic(state.get_namespace());

    info!("publishing to topic {}", hub_pub_topic);

    let rpc_message = serde_json::to_string(&RPCMessage {
        data: RPCMessageData::UpdateSettings(),
    })
    .map_err(|x| x.to_string())?;

    client
        .publish(hub_pub_topic, rpc_message)
        .await
        .map_err(|x| x.to_string())?;

    info!("pubsub update settings message sent");

    // 4. return result

    Ok(payload.settings)
}

async fn update_settings_for_publishers(
    payload: UpdateSettingsRequest,
    state: Arc<SharedState>,
) -> Result<MessageValue, String> {
    let client = state.redis.lock().await.clone().unwrap();

    // 1. encrypt settings

    let setting = state
        .env
        .encrypt(payload.settings.to_vec().unwrap())
        .map_err(|x| x.message)?;

    // 2. save encrypted in redis

    let publishers_key =
        topics::publisher_settings_key(state.get_namespace(), payload.name.clone());
    client
        .set(publishers_key.clone(), setting)
        .await
        .map_err(|x| x.to_string())?;

    // 3. notify all other publishers

    let pub_topic = topics::hub_to_publisher_pubsub_topic(state.get_namespace(), payload.name);

    info!("publishing to topic {}", pub_topic);

    let rpc_message = serde_json::to_string(&RPCMessage {
        data: RPCMessageData::UpdateSettings(),
    })
    .map_err(|x| x.to_string())?;

    client
        .publish(pub_topic, rpc_message)
        .await
        .map_err(|x| x.to_string())?;

    info!("pubsub update settings message sent");

    // 4. return result

    Ok(payload.settings)
}

pub async fn update_settings(
    Json(payload): Json<UpdateSettingsRequest>,
    state: Arc<SharedState>,
) -> impl IntoResponse {
    info!("[POST] Update settings");

    let update_fn: Result<MessageValue, String>;

    if state.env.name == payload.name {
        // update hub settings
        update_fn = update_settings_for_hub(payload, state).await;
    } else {
        // update publisher settings
        update_fn = update_settings_for_publishers(payload, state).await;
    }

    // return response
    match update_fn {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(err) => {
            warn!("error updating settings {err}");
            (
                StatusCode::BAD_REQUEST,
                Json(RhiaqeyError::create(400, err.to_string())),
            )
                .into_response()
        }
    }
}
