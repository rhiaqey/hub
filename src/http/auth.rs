use crate::http::state::SharedState;
use crate::hub::settings::HubSettingsApiKey;
use axum::extract::Query;
use axum::response::IntoResponse;
use http::{HeaderMap, StatusCode};
use log::{debug, trace, warn};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

#[derive(Debug, Deserialize)]
pub struct AuthenticationQueryParams {
    pub api_key: String,
    pub host: String,
}

pub fn extract_api_key_from_relative_path(relative_path: &str) -> Option<String> {
    let full = format!("http://localhost{}", relative_path);

    return match Url::parse(full.as_str()) {
        Ok(parts) => {
            trace!("we parsed full url into parts");

            let queries: HashMap<_, _> = parts.query_pairs().collect();
            if queries.contains_key("api_key") {
                let api_key = queries.get("api_key").unwrap().to_string();
                debug!("api_key was found");

                return Some(api_key);
            }

            warn!("could ot find api_key part");

            return None;
        }
        Err(e) => {
            warn!("error parsing api key {}", e);
            None
        }
    };
}

pub async fn valid_api_key(key: HubSettingsApiKey, state: Arc<SharedState>) -> bool {
    let settings = state.settings.read().unwrap();
    settings.security.api_keys.contains(&key)
}

pub async fn get_auth(
    _hostname: String,
    _headers: HeaderMap,
    _query: Query<Option<AuthenticationQueryParams>>,
    _state: Arc<SharedState>,
) -> impl IntoResponse {
    debug!("some from hostname {:?}", _hostname);
    debug!("some from headers {:?}", _headers);
    debug!("some from query {:?}", _query);
    (StatusCode::UNAUTHORIZED, "Unauthorized access")
}
