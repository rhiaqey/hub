use crate::http::state::{SharedState, UpdateSettingsRequest};
use crate::hub::settings::HubSettings;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;
use jsonschema::{Draft, JSONSchema};
use log::{debug, info, trace, warn};
use redis::Commands;
use rhiaqey_common::pubsub::{PublisherRegistrationMessage, RPCMessage, RPCMessageData};
use rhiaqey_common::{result::RhiaqeyResult, security, topics};
use rhiaqey_sdk_rs::message::MessageValue;
use serde_json::Value;
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

fn validate_settings_for_hub(message: &MessageValue) -> RhiaqeyResult<bool> {
    let raw = message.to_vec()?;
    let settings_value = serde_json::from_slice(raw.as_slice())?;

    let schema = HubSettings::schema();

    let compiled_schema = JSONSchema::options()
        .with_draft(Draft::Draft7)
        .compile(&schema)
        .map_err(|x| x.to_string())?;

    let result = match compiled_schema.validate(&settings_value) {
        Ok(_) => Ok(true),
        Err(errors) => {
            for error in errors {
                warn!("hub setting schema validation error: {}", error);
                warn!(
                    "hub setting schema validation instance path: {}",
                    error.instance_path
                );
            }

            Ok(false)
        }
    };

    if result.is_err() {
        return result;
    }

    trace!("checking for duplicate api keys");
    let settings: HubSettings = serde_json::from_value(settings_value)?;
    if !has_unique_elements(settings.security.api_keys.iter().map(|x| &x.api_key)) {
        return Err("duplicate api keys found".into());
    }

    result
}

fn update_settings_for_hub(
    payload: UpdateSettingsRequest,
    state: Arc<SharedState>,
) -> RhiaqeyResult<MessageValue> {
    debug!("hub settings payload {:?}", payload);

    let valid = validate_settings_for_hub(&payload.settings)?;
    trace!("hub settings valid: {valid}");

    if !valid {
        warn!("failed payload schema validation");
        return Err("Schema validation failed for payload".into());
    }

    trace!("encrypt settings for hub");

    let keys = state.security.read().unwrap();
    let data = payload.settings.to_vec()?;
    let settings = security::aes_encrypt(
        keys.no_once.as_slice(),
        keys.key.as_slice(),
        data.as_slice(),
    )?;

    let hub_settings_key = topics::hub_settings_key(state.get_namespace());
    let _ = state
        .redis_rs
        .lock()
        .unwrap()
        .set(hub_settings_key.clone(), settings)
        .map_err(|x| x.to_string())?;
    trace!("encrypted settings saved in redis");

    match state.publish_rpc_message(RPCMessageData::UpdateHubSettings()) {
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

fn validate_settings_for_publishers(message: &MessageValue, schema: Value) -> RhiaqeyResult<bool> {
    let raw = message.to_vec()?;
    let settings = serde_json::from_slice(raw.as_slice())?;

    let compiled_schema = JSONSchema::options()
        .with_draft(Draft::Draft7)
        .compile(&schema)
        .map_err(|x| x.to_string())?;

    let result = match compiled_schema.validate(&settings) {
        Ok(_) => Ok(true),
        Err(errors) => {
            for error in errors {
                debug!("publishers setting schema validation error: {}", error);
                debug!(
                    "publishers setting schema validation instance path: {}",
                    error.instance_path
                );
            }

            Ok(false)
        }
    };

    result
}

fn update_settings_for_publishers(
    payload: UpdateSettingsRequest,
    state: Arc<SharedState>,
) -> RhiaqeyResult<MessageValue> {
    trace!("find first schema for publisher");
    let name = payload.name;
    let namespace = state.get_namespace();
    let schema_key = topics::publisher_schema_key(namespace, name.clone(), payload.id);
    debug!("schema key {schema_key}");

    let lock = state.redis_rs.clone();
    let mut conn = lock.lock().unwrap();

    let schema_response: String = conn.get(schema_key)?;
    if schema_response == "" {
        return Err(format!("No schema found for {name}").into());
    }

    let schema: PublisherRegistrationMessage = serde_json::from_str(schema_response.as_str())?;
    let valid = validate_settings_for_publishers(&payload.settings, schema.schema)?;
    debug!("publishers settings valid: {valid}");

    if !valid {
        warn!("failed payload schema validation");
        return Err("Schema validation failed for payload".into());
    }

    trace!("encrypt settings for publishers");

    let keys = state.security.read().unwrap();
    let data = payload.settings.to_vec()?;
    let settings = security::aes_encrypt(
        keys.no_once.as_slice(),
        keys.key.as_slice(),
        data.as_slice(),
    )?;
    drop(keys);

    trace!("save encrypted in redis");

    let publishers_key = topics::publisher_settings_key(state.get_namespace(), name.clone());
    let _ = conn
        .set(publishers_key.clone(), settings)
        .map_err(|x| x.to_string())?;

    // 3. notify all other publishers

    let pub_topic = topics::hub_to_publisher_pubsub_topic(state.get_namespace(), name);

    info!("publishing to topic {}", pub_topic);

    let rpc_message = serde_json::to_string(&RPCMessage {
        data: RPCMessageData::UpdatePublisherSettings(),
    })
    .map_err(|x| x.to_string())?;

    let _ = conn
        .publish(pub_topic, rpc_message)
        .map_err(|x| x.to_string())?;

    info!("pubsub update settings message sent");

    // 4. return result

    Ok(payload.settings)
}

pub async fn update_settings(
    State(state): State<Arc<SharedState>>,
    Json(payload): Json<UpdateSettingsRequest>,
) -> impl IntoResponse {
    info!("[POST] Update settings");

    let update_fn: RhiaqeyResult<MessageValue>;

    if state.env.name == payload.name {
        trace!("update hub settings");
        update_fn = update_settings_for_hub(payload, state);
    } else {
        trace!("update publisher settings");
        update_fn = update_settings_for_publishers(payload, state);
    }

    // return response
    match update_fn {
        Ok(response) => {
            info!("settings updated successfully");
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(err) => err.into_response(),
    }
}
