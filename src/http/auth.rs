use crate::http::state::SharedState;
use crate::hub::settings::HubSettingsIPs;
use axum::extract::{Query, Host};
use axum::response::IntoResponse;
use axum::{extract::State, http::HeaderMap};
use axum_extra::extract::CookieJar;
use axum_extra::extract::cookie::Cookie;
use hyper::http::StatusCode;
use log::{debug, info, trace, warn};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;
use sha256::digest;
use axum_client_ip::InsecureClientIp;

#[derive(Debug, Deserialize)]
pub struct AuthenticationQueryParams {
    #[serde(rename = "api_key")]
    pub api_key: Option<String>,
    #[serde(rename = "host")]
    pub host: Option<String>,
}

pub fn extract_api_host_from_relative_path(relative_path: &str) -> Option<String> {
    trace!("extract host from relative path {relative_path}");

    let full = format!("http://localhost{}", relative_path);

    return match Url::parse(full.as_str()) {
        Ok(parts) => {
            trace!("we parsed full url {full} into parts");

            let queries: HashMap<_, _> = parts.query_pairs().collect();
            if queries.contains_key("host") {
                let host = queries.get("host").unwrap().to_string();
                debug!("host was found");

                return Some(host);
            }

            warn!("could not find host part");

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

pub fn get_hostname(hostname: String, headers: &HeaderMap) -> Option<String> {
    let mut hostname: Option<String> = Some(hostname);

    if headers.contains_key("x-forwarded-host") {
        debug!("x-forwarded-host header found");
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

pub fn get_api_key(
    qs: &Query<AuthenticationQueryParams>,
    headers: &HeaderMap,
    cookies: &CookieJar,
) -> Option<String> {
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
    } else if let Some(cookie) = cookies.get("x-api-key") {
        debug!("x-api-key cookie found");
        api_key = Some(cookie.to_string());
    }

    api_key
}

pub fn get_api_host(
    qs: &Query<AuthenticationQueryParams>,
    headers: &HeaderMap,
    cookies: &CookieJar,
) -> Option<String> {
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
    } else if let Some(host) = qs.host.clone() {
        debug!("host qs found");
        api_host = Some(host)
    } else if headers.contains_key("x-forwarded-uri") {
        debug!("x-forwarded-uri header found");
        api_host = extract_api_host_from_relative_path(
            headers.get("x-forwarded-uri").unwrap().to_str().unwrap(),
        )
    } else if let Some(cookie) = cookies.get("x-api-host") {
        debug!("x-api-host cookie found");
        api_host = Some(cookie.to_string());
    }

    api_host
}

pub async fn get_auth(
    headers: HeaderMap,                                         // external and internal headers
    insecure_ip: InsecureClientIp,                              // external
    // secure_ip: SecureClientIp,                               // internal
    Host(hostname): Host,                               // external host
    qs: Query<AuthenticationQueryParams>,                       // external query string
    State(state): State<Arc<SharedState>>,    // global state
    jar: CookieJar,                                             // global
) -> impl IntoResponse {
    trace!("[dump] headers: {:?}", headers);
    trace!("[dump] insecure ip: {:?}", insecure_ip);
    trace!("[dump] host: {:?}", hostname);
    trace!("[dump] qs: {:?}", qs);
    trace!("[dump] settings: {:?}", state.as_ref().settings);
    trace!("[dump] cookies: {:?}", jar);

    let api_host = get_api_host(&qs, &headers, &jar);
    if api_host.is_none() {
        warn!("api host was not found");
        return (StatusCode::UNAUTHORIZED, "Unauthorized access");
    }

    let api_key = get_api_key(&qs, &headers, &jar);
    if api_key.is_none() {
        warn!("api key was not found");
        return (StatusCode::UNAUTHORIZED, "Unauthorized access");
    }

    let hostname = get_hostname(hostname, &headers);
    if hostname.is_none() {
        warn!("hostname was not found");
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

    let ip = insecure_ip.0.to_string();

    if valid_api_key(
        digest(api_key.clone().unwrap()),
        api_host.clone().unwrap(),
        ip,
        state,
    )
    .await
    {
        info!("granting access");
        let _ = jar
            .add(Cookie::new("x-api-key", api_key.unwrap()))
            .add(Cookie::new("x-api-host", api_host.unwrap()));
        (StatusCode::OK, "Access granted")
    } else {
        warn!("forbidden access");
        (StatusCode::UNAUTHORIZED, "Unauthorized access")
    }
}
