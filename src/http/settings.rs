use crate::http::state::{SharedState, UpdateSettingsRequest};
use crate::hub::settings::HubSettings;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::Json;
use hyper::StatusCode;
use jsonschema::{Draft, JSONSchema};
use log::{debug, info, trace, warn};
use rhiaqey_common::error::RhiaqeyError;
use rhiaqey_common::pubsub::{PublisherRegistrationMessage, RPCMessage, RPCMessageData};
use rhiaqey_common::{security, topics};
use rhiaqey_sdk_rs::message::MessageValue;
use rustis::commands::{PubSubCommands, StringCommands};
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

fn validate_settings_for_hub(message: &MessageValue) -> Result<bool, RhiaqeyError> {
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
        return Err(RhiaqeyError::from("duplicate api keys found"));
    }

    result
}

async fn update_settings_for_hub(
    payload: UpdateSettingsRequest,
    state: Arc<SharedState>,
) -> Result<MessageValue, RhiaqeyError> {
    debug!("hub settings payload {:?}", payload);

    let valid = validate_settings_for_hub(&payload.settings)?;
    trace!("hub settings valid: {valid}");

    if !valid {
        warn!("failed payload schema validation");
        return Err(RhiaqeyError::from("Schema validation failed for payload"));
    }

    let client = state.redis.lock().await.clone().unwrap();

    trace!("encrypt settings for hub");

    let keys = state.security.lock().await;
    let data = payload.settings.to_vec()?;
    let settings = security::aes_encrypt(
        keys.no_once.as_slice(),
        keys.key.as_slice(),
        data.as_slice(),
    )
    .map_err(|x| x.message)?;
    drop(keys);

    trace!("save encrypted in redis");

    let hub_settings_key = topics::hub_settings_key(state.get_namespace());
    client
        .set(hub_settings_key.clone(), settings)
        .await
        .map_err(|x| x.to_string())?;

    trace!("notify all other hubs");

    let hub_pub_topic = topics::hub_raw_to_hub_clean_pubsub_topic(state.get_namespace());
    debug!("publishing to topic {}", hub_pub_topic);

    let rpc_message = serde_json::to_string(&RPCMessage {
        data: RPCMessageData::UpdateSettings(),
    })
    .map_err(|x| x.to_string())?;

    client
        .publish(hub_pub_topic, rpc_message)
        .await
        .map_err(|x| x.to_string())?;

    info!("pubsub update settings message sent");

    Ok(payload.settings)
}

fn validate_settings_for_publishers(
    message: &MessageValue,
    schema: Value,
) -> Result<bool, RhiaqeyError> {
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

async fn update_settings_for_publishers(
    payload: UpdateSettingsRequest,
    state: Arc<SharedState>,
) -> Result<MessageValue, RhiaqeyError> {
    let client = state.redis.lock().await.clone().unwrap();

    trace!("find first schema for publisher");
    let name = payload.name;
    let namespace = state.get_namespace();
    let schema_key = topics::publisher_schema_key(namespace, name.clone());
    debug!("schema key {schema_key}");

    let schema_response: String = client.get(schema_key).await?;
    if schema_response == "" {
        return Err(RhiaqeyError::from(format!("No schema found for {name}")));
    }

    let schema: PublisherRegistrationMessage = serde_json::from_str(schema_response.as_str())?;
    let valid = validate_settings_for_publishers(&payload.settings, schema.schema)?;
    debug!("publishers settings valid: {valid}");

    if !valid {
        warn!("failed payload schema validation");
        return Err(RhiaqeyError::from(
            "Schema validation failed for payload".to_string(),
        ));
    }

    trace!("encrypt settings for publishers");

    let keys = state.security.lock().await;
    let data = payload.settings.to_vec()?;
    let settings = security::aes_encrypt(
        keys.no_once.as_slice(),
        keys.key.as_slice(),
        data.as_slice(),
    )
    .map_err(|x| x.message)?;
    drop(keys);

    trace!("save encrypted in redis");

    let publishers_key = topics::publisher_settings_key(state.get_namespace(), name.clone());
    client
        .set(publishers_key.clone(), settings)
        .await
        .map_err(|x| x.to_string())?;

    // 3. notify all other publishers

    let pub_topic = topics::hub_to_publisher_pubsub_topic(state.get_namespace(), name);

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
    State(state): State<Arc<SharedState>>,
    Json(payload): Json<UpdateSettingsRequest>,
) -> impl IntoResponse {
    info!("[POST] Update settings");

    let update_fn: Result<MessageValue, RhiaqeyError>;

    if state.env.name == payload.name {
        trace!("update hub settings");
        update_fn = update_settings_for_hub(payload, state).await;
    } else {
        trace!("update publisher settings");
        update_fn = update_settings_for_publishers(payload, state).await;
    }

    // return response
    match update_fn {
        Ok(response) => {
            info!("settings updated successfully");
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(err) => {
            warn!("error updating settings: {err}");
            (
                StatusCode::BAD_REQUEST,
                Json(RhiaqeyError::create(400, err.to_string())),
            )
                .into_response()
        }
    }
}
