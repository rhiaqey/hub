use crate::http::state::SharedState;
use crate::hub::settings::HubSettingsApiKey;
use axum::extract::Query;
use axum::{http::StatusCode, response::IntoResponse};
use http::HeaderMap;
use log::{debug, info, trace, warn};
use serde::Deserialize;
use sha256::digest;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

#[derive(Debug, Deserialize)]
pub struct AuthenticationQueryParams {
    pub api_key: Option<String>,
}

pub fn extract_api_key(relative_path: &str) -> Option<String> {
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

pub async fn valid_api_key(key: String, state: Arc<SharedState>) -> bool {
    let settings = state.settings.read().unwrap();
    settings.api_keys.contains(&HubSettingsApiKey {
        api_key: digest(key),
        // domains: vec![],
    })
}

pub async fn get_auth(
    headers: HeaderMap,
    query: Query<AuthenticationQueryParams>,
    state: Arc<SharedState>,
) -> impl IntoResponse {
    info!("authenticating with api_key");

    if let Some(query_api_key) = query.api_key.clone() {
        info!("query api key found");

        return if valid_api_key(query_api_key, state).await {
            (StatusCode::OK, "OK")
        } else {
            (StatusCode::UNAUTHORIZED, "Unauthorized access")
        };
    }

    warn!("api key was not found in the url");

    if headers.contains_key("x-forwarded-uri") {
        let path = headers.get("x-forwarded-uri").unwrap().to_str().unwrap();

        info!("x-forwarded-uri found {}", path);

        if let Some(api_key) = extract_api_key(path) {
            info!("api key extracted successfully from x-forwarded-uri");

            if valid_api_key(api_key, state).await {
                return (StatusCode::OK, "OK");
            }
        }
    }

    warn!("api key was not found in the x-forwarded-uri");

    (StatusCode::UNAUTHORIZED, "Unauthorized access")
}
