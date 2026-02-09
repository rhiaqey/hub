use crate::http::common::retrieve_schema_for_hub;
use crate::http::state::{SharedState, UpdateSettingsRequest};
use crate::hub::settings::HubSettings;
use anyhow::{Context, bail};
use axum::Json;
use axum::extract::State;
use axum::response::IntoResponse;
use hyper::StatusCode;
use log::{debug, info, trace, warn};
use redis::{Commands, RedisResult};
use rhiaqey_common::pubsub::{PublisherRegistrationMessage, RPCMessage, RPCMessageData};
use rhiaqey_common::topics;
use rhiaqey_sdk_rs::message::MessageValue;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;

fn has_unique_elements<T>(iter: T) -> bool
where
    T: IntoIterator,
    T::Item: Eq + Hash,
{
    let mut uniq = HashSet::new();
    iter.into_iter().all(move |x| uniq.insert(x))
}

fn validate_settings_for_hub(message: &MessageValue, schema: Value) -> bool {
    let Ok(raw) = message.to_vec() else {
        return false;
    };

    let Ok(settings) = serde_json::from_slice(raw.as_slice()) else {
        return false;
    };

    let result = jsonschema::is_valid(&schema, &settings);

    trace!("checking for duplicate api keys");

    let Ok(settings) = serde_json::from_value::<HubSettings>(settings) else {
        warn!("failed to deserialize from value");
        return false;
    };

    if !has_unique_elements(settings.security.api_keys.iter().map(|x| &x.api_key)) {
        warn!("duplicate api keys found");
        return false;
    }

    result
}

pub async fn update_settings_for_hub(
    payload: UpdateSettingsRequest,
    state: Arc<SharedState>,
) -> anyhow::Result<MessageValue> {
    debug!("hub settings payload {:?}", payload);

    let schema = retrieve_schema_for_hub(state.get_namespace(), state.redis_rs.clone()).await?;

    trace!("hub schema retrieved");

    let valid = validate_settings_for_hub(&payload.settings, schema);
    trace!("hub settings valid: {valid}");

    if !valid {
        warn!("failed payload schema validation");
        bail!("Schema validation failed for payload")
    }

    trace!("encrypt settings for hub");

    let hub_settings_key = topics::hub_settings_key(state.get_namespace());
    let data = payload.settings.to_vec()?;
    state.store_settings(hub_settings_key, data)?;

    trace!("save encrypted in redis");

    match state.publish_rpc_message(RPCMessageData::UpdateHubSettings(vec![])) {
        Ok(_) => {
            info!("pubsub update settings message sent");
            Ok(payload.settings)
        }
        Err(err) => {
            warn!("error publishing update hub settings pubsub: {err}");
            Err(err)
        }
    }
}

fn validate_settings_for_publishers(message: &MessageValue, schema: Value) -> bool {
    let Ok(raw) = message.to_vec() else {
        return false;
    };

    let Ok(settings) = serde_json::from_slice(raw.as_slice()) else {
        return false;
    };

    jsonschema::is_valid(&schema, &settings)
}

fn retrieve_schema_for_publisher(
    name: String,
    schema_key: String,
    state: Arc<SharedState>,
) -> anyhow::Result<String> {
    debug!("retrieve schema for publisher");

    let lock = state.redis_rs.clone();
    let mut conn = lock.lock().unwrap();

    let schema_redis_response: RedisResult<String> = conn.get(schema_key);
    if schema_redis_response.is_err() {
        bail!(format!("No schema found for {name}"))
    }

    let schema_response: String = schema_redis_response.unwrap();
    if schema_response == "" {
        bail!(format!("No schema found for {name}"))
    }

    Ok(schema_response)
}

pub fn update_settings_for_publishers(
    payload: UpdateSettingsRequest,
    state: Arc<SharedState>,
) -> anyhow::Result<MessageValue> {
    trace!("find first schema for publisher");
    let name = payload.name;
    let namespace = state.get_namespace();
    let schema_key = topics::publisher_schema_key(namespace, name.as_ref());
    debug!("schema key {schema_key}");

    let schema_response: String =
        retrieve_schema_for_publisher(name.clone(), schema_key, state.clone())?;
    debug!("schema retrieved");

    let schema: PublisherRegistrationMessage = serde_json::from_str(schema_response.as_str())?;
    let valid = validate_settings_for_publishers(&payload.settings, schema.schema);
    debug!("publishers settings for id={} is valid: {valid}", schema.id);

    if !valid {
        warn!("failed payload schema validation");
        bail!("Schema validation failed for payload")
    }

    // 2. store settings

    trace!("encrypt settings for publishers");

    let publishers_key = topics::publisher_settings_key(state.get_namespace(), name.as_ref());
    let data = payload.settings.to_vec()?;
    state.store_settings(publishers_key, data)?;

    trace!("save encrypted in redis");

    // 3. notify all other publishers

    let pub_topic = topics::hub_to_publisher_pubsub_topic(state.get_namespace(), name.as_ref());

    info!("publishing to topic {}", pub_topic);

    let rpc_message = RPCMessage {
        data: RPCMessageData::UpdatePublisherSettings(vec![]),
    }
    .ser_to_string()
    .context("failed to serialize rpc message")?;

    let lock = state.redis_rs.clone();
    let mut conn = lock.lock().unwrap();

    trace!("redis connection acquired");

    let _: () = conn
        .publish(pub_topic, rpc_message)
        .context("failed to publish message")?;

    info!("pubsub update settings message sent");

    // 4. return result

    Ok(payload.settings)
}

pub async fn update_hub_settings_handler(
    State(state): State<Arc<SharedState>>,
    Json(payload): Json<UpdateSettingsRequest>,
) -> impl IntoResponse {
    info!("[POST] Update hub settings");

    match update_settings_for_hub(payload, state).await {
        Ok(response) => {
            info!("hub settings updated successfully");
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(err) => {
            warn!("error updating hub settings: {}", err);
            return (
                StatusCode::OK,
                [(hyper::header::CONTENT_TYPE, "application/json")],
                json!({
                    "code": 500,
                    "message": "failed to update hub settings"
                })
                .to_string(),
            )
                .into_response();
        }
    }
}

pub async fn update_publishers_settings_handler(
    State(state): State<Arc<SharedState>>,
    Json(payload): Json<UpdateSettingsRequest>,
) -> impl IntoResponse {
    info!("[POST] Update publisher settings");

    match update_settings_for_publishers(payload, state) {
        Ok(response) => {
            info!("publisher settings updated successfully");
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(err) => {
            warn!("error updating publisher settings: {}", err);
            return (
                StatusCode::OK,
                [(hyper::header::CONTENT_TYPE, "application/json")],
                json!({
                    "code": 500,
                    "message": "failed to update publisher settings"
                })
                .to_string(),
            )
                .into_response();
        }
    }
}
