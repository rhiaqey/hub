use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HubSettingsIPs {
    #[serde(alias = "Blacklisted")]
    Blacklisted(Vec<String>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HubSettingsApiKey {
    #[serde(alias = "ApiKey")]
    pub api_key: String,
    #[serde(alias = "Hosts")]
    pub hosts: Vec<String>,
    #[serde(alias = "IPs")]
    pub ips: Option<HubSettingsIPs>,
}

impl HubSettingsApiKey {
    pub fn vecs_match<T: PartialEq>(a: &Vec<T>, b: &Vec<T>) -> bool {
        let matching = a.iter().zip(b.iter()).filter(|&(a, b)| a == b).count();
        matching == a.len() && matching == b.len()
    }
}

impl PartialEq for HubSettingsApiKey {
    fn eq(&self, other: &Self) -> bool {
        if !self.api_key.eq(&other.api_key) {
            return false;
        }

        if !Self::vecs_match(&self.hosts, &other.hosts) {
            return false;
        }

        let self_ips = self.ips.clone();
        let other_ips = other.ips.clone();

        if self_ips.is_none() && other_ips.is_none() {
            return true; // both do not exist
        }

        if self_ips.is_some() && other_ips.is_none() {
            return false; // one of them exist
        }

        if self_ips.is_none() && other_ips.is_some() {
            return false; // one of them exist
        }

        match self_ips.unwrap() {
            HubSettingsIPs::Blacklisted(self_blacklisted_ips) => match other_ips.unwrap() {
                HubSettingsIPs::Blacklisted(other_blacklisted_ips) => self_blacklisted_ips
                    .iter()
                    .any(|self_blacklisted_ip| other_blacklisted_ips.contains(self_blacklisted_ip)),
            },
        }
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct HubSecurity {
    #[serde(alias = "ApiKeys")]
    pub api_keys: Vec<HubSettingsApiKey>,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct HubSettings {
    #[serde(alias = "Security")]
    pub security: HubSecurity,
}

impl HubSettings {
    pub fn schema() -> Value {
        json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "Security": {
                    "type": "object",
                    "properties": {
                        "ApiKeys": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "ApiKey": {
                                        "type": "string",
                                        "examples": [ "strong-api-key" ]
                                    },
                                    "Hosts": {
                                        "type": "array",
                                        "items": {
                                            "type": "string",
                                            "examples": [ "http://localhost:3333", "https://192.168.0.1:8080" ]
                                        }
                                    },
                                    "IPs": {
                                        "type": "object",
                                        "properties": {
                                            "Blacklisted": {
                                                "type": "array",
                                                "items": {
                                                    "type": "string",
                                                    "format": "ipv4",
                                                    "examples": [ "192.168.0.1", "10.0.0.1" ]
                                                }
                                            }
                                        }
                                    }
                                },
                                "required": [ "ApiKey", "Hosts" ],
                                "additionalProperties": false
                            }
                        }
                    },
                    "required": [ "ApiKeys" ],
                    "additionalProperties": false
                }
            },
            "required": [ "Security" ],
            "additionalProperties": false
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::hub::settings::{HubSettingsApiKey, HubSettingsIPs};

    #[test]
    fn can_serialize() {
        let key1 = HubSettingsApiKey {
            api_key: "abc".to_string(),
            hosts: vec!["localhost".to_string()],
            ips: None,
        };

        let result = serde_json::to_string(&key1);
        assert_eq!(result.is_ok(), true);
        println!("{}", result.unwrap());
    }

    #[test]
    fn can_serialize_blacklisted_ips() {
        let key1 = HubSettingsApiKey {
            api_key: "abc".to_string(),
            hosts: vec!["localhost".to_string()],
            ips: Some(HubSettingsIPs::Blacklisted(vec!["192.168.0.3".to_string()])),
        };

        let result = serde_json::to_string(&key1);
        assert_eq!(result.is_ok(), true);
        println!("{}", result.unwrap());
    }

    #[test]
    fn partial_eq_works_with_no_ips() {
        let key1 = HubSettingsApiKey {
            api_key: "abc".to_string(),
            hosts: vec!["localhost".to_string()],
            ips: None,
        };

        let key2 = HubSettingsApiKey {
            api_key: "abc".to_string(),
            hosts: vec!["localhost".to_string()],
            ips: None,
        };

        assert_eq!(key1, key2);
    }

    #[test]
    fn partial_eq_works_with_different_api_keys() {
        let key1 = HubSettingsApiKey {
            api_key: "abc".to_string(),
            hosts: vec!["localhost".to_string()],
            ips: None,
        };
        let key2 = HubSettingsApiKey {
            api_key: "def".to_string(),
            hosts: vec!["localhost".to_string()],
            ips: None,
        };
        assert_ne!(key1, key2);
    }

    #[test]
    fn partial_eq_works_with_different_hosts() {
        let key1 = HubSettingsApiKey {
            api_key: "abc".to_string(),
            hosts: vec!["localhost".to_string()],
            ips: None,
        };

        let key2 = HubSettingsApiKey {
            api_key: "abc".to_string(),
            hosts: vec!["local.host".to_string()],
            ips: None,
        };

        assert_ne!(key1, key2);
    }

    #[test]
    fn partial_eq_works_with_same_blacklisted_ips() {
        let key1: HubSettingsApiKey = HubSettingsApiKey {
            api_key: "abc".to_string(),
            hosts: vec!["localhost".to_string()],
            ips: Some(HubSettingsIPs::Blacklisted(vec!["192.168.0.1".to_string()])),
        };

        let key2 = HubSettingsApiKey {
            api_key: "abc".to_string(),
            hosts: vec!["localhost".to_string()],
            ips: Some(HubSettingsIPs::Blacklisted(vec!["192.168.0.1".to_string()])),
        };

        assert_eq!(key1, key2);
    }

    #[test]
    fn partial_eq_works_with_different_blacklisted_ips() {
        let key1 = HubSettingsApiKey {
            api_key: "abc".to_string(),
            hosts: vec!["localhost".to_string()],
            ips: Some(HubSettingsIPs::Blacklisted(vec!["192.168.0.1".to_string()])),
        };
        let key2 = HubSettingsApiKey {
            api_key: "abc".to_string(),
            hosts: vec!["localhost".to_string()],
            ips: Some(HubSettingsIPs::Blacklisted(vec!["192.168.0.2".to_string()])),
        };
        assert_ne!(key1, key2);
    }

    #[test]
    fn partial_eq_works_with_multiple_different_blacklisted_ips() {
        let key1 = HubSettingsApiKey {
            api_key: "abc".to_string(),
            hosts: vec!["localhost".to_string()],
            ips: Some(HubSettingsIPs::Blacklisted(vec!["192.168.0.1".to_string()])),
        };
        let key2 = HubSettingsApiKey {
            api_key: "abc".to_string(),
            hosts: vec!["localhost".to_string()],
            ips: Some(HubSettingsIPs::Blacklisted(vec![
                "192.168.0.1".to_string(),
                "192.168.0.2".to_string(),
            ])),
        };
        assert_eq!(key1, key2);
    }
}
