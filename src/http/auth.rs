use crate::http::state::SharedState;
use crate::hub::settings::HubSettingsIPs;
use axum::extract::Query;
use axum::response::IntoResponse;
use axum::TypedHeader;
use headers_client_ip::XRealIP;
use http::{HeaderMap, StatusCode};
use log::{debug, info, trace, warn};
use serde::Deserialize;
use sha256::digest;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

#[derive(Debug, Deserialize)]
pub struct AuthenticationQueryParams {
    pub api_key: Option<String>,
    pub host: Option<String>,
}

pub fn extract_api_host_from_relative_path(relative_path: &str) -> Option<String> {
    let full = format!("http://localhost{}", relative_path);

    return match Url::parse(full.as_str()) {
        Ok(parts) => {
            trace!("we parsed full url into parts");

            let queries: HashMap<_, _> = parts.query_pairs().collect();
            if queries.contains_key("host") {
                let api_key = queries.get("host").unwrap().to_string();
                debug!("host was found");

                return Some(api_key);
            }

            warn!("could ot find host part");

            return None;
        }
        Err(e) => {
            warn!("error parsing host {}", e);
            None
        }
    };
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

pub async fn valid_api_key(
    api_key: String,
    api_host: String,
    ip: String,
    state: Arc<SharedState>,
) -> bool {
    let settings = state.settings.read().unwrap();

    debug!("api creds {} - {} - {}", api_key, api_host, ip.to_string());

    let security_api_key = settings
        .security
        .api_keys
        .iter()
        .find(|key| key.api_key == api_key && key.host == api_host);

    if security_api_key.is_some() {
        debug!("security api key found");
    } else {
        warn!("security api key not found");
    }

    if let Some(xxx) = security_api_key {
        let xip = xxx.ips.clone();

        if xip.is_none() {
            warn!("security api key does not contain any ip requirements");
            return true;
        }

        if let Some(xyz) = xip {
            return match xyz {
                HubSettingsIPs::Whitelisted(ips) => {
                    let result = ips.contains(&ip);

                    if result {
                        debug!("ip is whitelisted {}", result);
                    }

                    result
                }
                HubSettingsIPs::Blacklisted(ips) => {
                    let result = ips.contains(&ip);

                    if result {
                        warn!("ip is blacklisted {}", result);
                    }

                    !result
                }
            };
        }
    }

    return false;
}

pub fn get_ip(real_ip: Option<TypedHeader<XRealIP>>, headers: &HeaderMap) -> Option<String> {
    let mut ip: Option<String> = None;

    if real_ip.is_some() {
        debug!("x-real-ip found");
        ip = Some(real_ip.unwrap().to_string());
    } else if headers.contains_key("x-forwarded-for") {
        debug!("x-forwarded-for detected");
        ip = Some(
            headers
                .get("x-forwarded-for")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string(),
        );
    }

    ip
}

pub fn get_hostname(hostname: String, headers: &HeaderMap) -> Option<String> {
    let mut hostname: Option<String> = Some(hostname);

    if headers.contains_key("x-forwarded-host") {
        debug!("header contains x-forwarded-host");
        hostname = Some(
            headers
                .get("x-forwarded-host")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string(),
        );
    }

    hostname
}

pub fn get_api_key(qs: &Query<AuthenticationQueryParams>, headers: &HeaderMap) -> Option<String> {
    let mut api_key: Option<String> = None;

    if headers.contains_key("x-api-key") {
        debug!("header contains x-api-key");
        api_key = Some(
            headers
                .get("x-api-key")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string(),
        );
    } else if let Some(key) = qs.api_key.clone() {
        debug!("qa contains api key");
        api_key = Some(key)
    } else if headers.contains_key("x-forwarded-uri") {
        debug!("forwarded-uri found");
        api_key = extract_api_key_from_relative_path(
            headers.get("x-forwarded-uri").unwrap().to_str().unwrap(),
        )
    }

    api_key
}

pub fn get_api_host(qs: &Query<AuthenticationQueryParams>, headers: &HeaderMap) -> Option<String> {
    let mut api_host: Option<String> = None;

    if headers.contains_key("x-api-host") {
        debug!("header contains x-api-host");
        api_host = Some(
            headers
                .get("x-api-host")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string(),
        );
    } else if let Some(host) = qs.host.clone() {
        debug!("qa contains host");
        api_host = Some(host)
    } else if headers.contains_key("x-forwarded-uri") {
        debug!("forwarded-uri found");
        api_host = extract_api_host_from_relative_path(
            headers.get("x-forwarded-uri").unwrap().to_str().unwrap(),
        )
    }

    api_host
}

pub async fn get_auth(
    hostname: String,
    ip: Option<TypedHeader<XRealIP>>,
    headers: HeaderMap,
    qs: Query<AuthenticationQueryParams>,
    state: Arc<SharedState>,
) -> impl IntoResponse {
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

    let hostname = get_hostname(hostname, &headers);
    if hostname.is_none() {
        warn!("failed to identify hostname");
        return (StatusCode::UNAUTHORIZED, "Unauthorized access");
    } else {
        let source_host = hostname.unwrap();
        let target_host = api_host.clone().unwrap();
        if source_host != target_host {
            warn!(
                "api host {} was different from hostname {}",
                target_host, source_host
            );
            return (StatusCode::UNAUTHORIZED, "Unauthorized access");
        }
    }

    let ip = get_ip(ip, &headers);
    if ip.is_none() {
        warn!("failed to extract ip");
        return (StatusCode::UNAUTHORIZED, "Unauthorized access");
    }

    if valid_api_key(
        digest(api_key.unwrap()),
        api_host.unwrap(),
        ip.unwrap(),
        state,
    )
    .await
    {
        info!("granting access");
        (StatusCode::OK, "Access granted")
    } else {
        warn!("forbidden access");
        (StatusCode::UNAUTHORIZED, "Unauthorized access")
    }
}
