use crate::http::state::SharedState;
use axum::extract::Query;
use axum::response::IntoResponse;
use axum::{extract::State, http::HeaderMap};
use axum_client_ip::XRealIp as ClientIp;
use hyper::http::StatusCode;
use log::{debug, info, trace, warn};
use serde::Deserialize;
use sha256::digest;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

#[cfg(not(debug_assertions))]
use crate::hub::settings::HubSettingsIPs;

#[derive(Debug, Deserialize)]
pub struct AuthenticationQueryParams {
    #[serde(rename = "api_key")]
    pub api_key: Option<String>,
    #[serde(rename = "api_host")]
    pub api_host: Option<String>,
}

pub fn extract_api_host_from_relative_path(relative_path: &str) -> Option<String> {
    trace!("extract host from relative path {relative_path}");

    let full = format!("http://localhost{}", relative_path);

    match Url::parse(full.as_str()) {
        Ok(parts) => {
            trace!("we parsed full url {full} into parts");

            let queries: HashMap<_, _> = parts.query_pairs().collect();
            if queries.contains_key("api_host") {
                let host = queries.get("api_host").unwrap().to_string();
                debug!("api host was found");

                return Some(host);
            }

            warn!("could not find host part");

            None
        }
        Err(e) => {
            warn!("error parsing host {}", e);
            None
        }
    }
}

pub fn extract_api_key_from_relative_path(relative_path: &str) -> Option<String> {
    let full = format!("http://localhost{}", relative_path);

    match Url::parse(full.as_str()) {
        Ok(parts) => {
            trace!("we parsed full url into parts");

            let queries: HashMap<_, _> = parts.query_pairs().collect();
            if queries.contains_key("api_key") {
                let api_key = queries.get("api_key").unwrap().to_string();
                debug!("api_key was found");

                return Some(api_key);
            }

            warn!("could not find api_key part");

            None
        }
        Err(e) => {
            warn!("error parsing api key {}", e);
            None
        }
    }
}

#[cfg(debug_assertions)]
pub fn valid_api_key(
    _api_key: String,
    _api_host: String,
    _ip: String,
    _state: Arc<SharedState>,
) -> bool {
    debug!("[DEBUG]: checking is key/host is valid");
    true
}

#[cfg(not(debug_assertions))]
pub fn valid_api_key(
    api_key: String,
    api_host: String,
    ip: String,
    state: Arc<SharedState>,
) -> bool {
    debug!("[RELEASE]: checking is key/host is valid");

    let settings = state.settings.read().unwrap();

    let security_api_key = settings
        .security
        .api_keys
        .iter()
        .find(|key| key.api_key == api_key && key.hosts.contains(&api_host));

    let Some(api_key) = security_api_key else {
        warn!("security api key not found for host {}", api_host);
        return false;
    };

    debug!("api key found for host");

    let Some(ref ips) = api_key.ips else {
        debug!("security api key does not contain any ip requirements");
        return true;
    };

    match ips {
        HubSettingsIPs::Blacklisted(blacklisted_ips) => !blacklisted_ips.contains(&ip),
    }
}

pub fn get_api_key(qs: &Query<AuthenticationQueryParams>, headers: &HeaderMap) -> Option<String> {
    let mut api_key: Option<String> = None;

    if headers.contains_key("x-api-key") {
        debug!("x-api-key header found");
        api_key = Some(
            headers
                .get("x-api-key")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string(),
        );
    } else if let Some(key) = qs.api_key.clone() {
        debug!("api_key qs found");
        api_key = Some(key)
    } else if headers.contains_key("x-forwarded-uri") {
        debug!("x-forwarded-uri header found");
        api_key = extract_api_key_from_relative_path(
            headers.get("x-forwarded-uri").unwrap().to_str().unwrap(),
        )
    }

    api_key
}

pub fn get_api_host(qs: &Query<AuthenticationQueryParams>, headers: &HeaderMap) -> Option<String> {
    let mut api_host: Option<String> = None;

    if headers.contains_key("x-api-host") {
        debug!("x-api-host header found");
        api_host = Some(
            headers
                .get("x-api-host")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string(),
        );
    } else if let Some(host) = qs.api_host.clone() {
        debug!("host qs found");
        api_host = Some(host)
    } else if headers.contains_key("x-forwarded-uri") {
        debug!("x-forwarded-uri header found");
        api_host = extract_api_host_from_relative_path(
            headers.get("x-forwarded-uri").unwrap().to_str().unwrap(),
        )
    }

    api_host
}

pub fn get_origin(headers: &HeaderMap) -> Option<String> {
    if headers.contains_key("origin") {
        return Some(headers.get("origin").unwrap().to_str().unwrap().to_string());
    }

    None
}

#[cfg(debug_assertions)]
pub fn valid_api_host(api_host: String, origin: String) -> bool {
    debug!(
        "[DEBUG]: check if valid host {} against {}",
        api_host, origin
    );
    true
}

#[cfg(not(debug_assertions))]
pub fn valid_api_host(api_host: String, origin: String) -> bool {
    debug!(
        "[RELEASE]: check if valid host {} against {}",
        api_host, origin
    );

    match Url::parse(origin.as_str()) {
        Ok(parts) => {
            trace!("url parsed into parts");

            if let Some(host) = parts.host_str() {
                let mut host = host.to_string();

                trace!("host extracted: {host}");
                trace!("port: {:?}", parts.port());
                trace!("port or default: {:?}", parts.port_or_known_default());

                if let Some(port) = parts.port() {
                    trace!("port found: {port}");
                    host = format!("{}:{}", host, port);
                }

                debug!("comparing {} with {}", api_host, host);

                return api_host.eq(&host);
            }

            false
        }
        Err(err) => {
            warn!("error parsing url: {}", err);
            false
        }
    }
}

pub async fn get_auth_handler(
    headers: HeaderMap,        // external and internal headers
    ClientIp(user_ip): ClientIp,
    // internal: SecureClientIp, // internal
    // Host(hostname): Host,               // external host
    qs: Query<AuthenticationQueryParams>, // external query string
    State(state): State<Arc<SharedState>>, // global state
) -> impl IntoResponse {
    info!("[GET] Handle auth");

    // trace!("[dump] headers: {:?}", headers);
    // trace!("[dump] secure ip: {:?}", internal);
    // trace!("[dump] insecure ip: {:?}", user_ip);
    // trace!("[dump] host: {:?}", hostname);
    // trace!("[dump] qs: {:?}", qs);
    // trace!("[dump] settings: {:?}", state.as_ref().settings);

    let api_host = get_api_host(&qs, &headers);
    if api_host.is_none() {
        warn!("api host was not found");
        return (StatusCode::UNAUTHORIZED, "Unauthorized access");
    }

    let api_key = get_api_key(&qs, &headers);
    if api_key.is_none() {
        warn!("api key was not found");
        return (StatusCode::UNAUTHORIZED, "Unauthorized access");
    }

    if let Some(origin) = get_origin(&headers) {
        debug!("origin found {}", origin);
        if valid_api_host(api_host.clone().unwrap(), origin) {
            debug!("api host is valid");
        } else {
            warn!("forbidden access");
            return (StatusCode::UNAUTHORIZED, "Unauthorized access");
        }
    } else {
        warn!("origin could not be found");
    }

    if valid_api_key(
        digest(api_key.clone().unwrap()),
        api_host.clone().unwrap(),
        user_ip.to_string(),
        state,
    ) {
        info!("api key is valid");
        (StatusCode::OK, "Access granted")
    } else {
        warn!("forbidden access");
        (StatusCode::UNAUTHORIZED, "Unauthorized access")
    }
}
