use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HubSettingsApiKey {
    #[serde(alias = "Key")]
    pub api_key: String,
}

impl PartialEq for HubSettingsApiKey {
    fn eq(&self, other: &Self) -> bool {
        self.api_key == other.api_key
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HubSettingsApiKey2 {
    #[serde(alias = "ApiKey")]
    pub api_key: String,
    #[serde(alias = "Domains")]
    pub domains: Vec<String>,
}

impl PartialEq for HubSettingsApiKey2 {
    fn eq(&self, other: &Self) -> bool {
        if self.api_key != other.api_key {
            return false;
        }

        if self.domains.is_empty() && other.domains.is_empty() {
            return true;
        }

        self.domains // at least one common
            .iter()
            .any(|x| other.domains.contains(&x))
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct HubSecurity {
    #[serde(alias = "ApiKeys")]
    pub api_keys: Vec<HubSettingsApiKey>,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct HubSettings {
    #[serde(alias = "ApiKeys")]
    pub api_keys: Vec<HubSettingsApiKey>,
    #[serde(alias = "Domains")]
    pub domains: Option<Vec<String>>,
}

#[cfg(test)]
mod tests {
    use crate::hub::settings::HubSettingsApiKey2;

    #[test]
    fn partial_eq_works_with_no_domains() {
        let key1 = HubSettingsApiKey2 {
            api_key: "abc".to_string(),
            domains: vec![],
        };
        let key2 = HubSettingsApiKey2 {
            api_key: "abc".to_string(),
            domains: vec![],
        };
        assert_eq!(key1, key2);
    }

    #[test]
    fn partial_eq_works_with_same_domains() {
        let key1 = HubSettingsApiKey2 {
            api_key: "abc".to_string(),
            domains: vec!["localhost".to_string()],
        };
        let key2 = HubSettingsApiKey2 {
            api_key: "abc".to_string(),
            domains: vec!["localhost".to_string()],
        };
        assert_eq!(key1, key2);
    }

    #[test]
    fn partial_eq_works_with_same_domains2() {
        let key1 = HubSettingsApiKey2 {
            api_key: "abc".to_string(),
            domains: vec!["localhost".to_string(), "localhost2".to_string()],
        };
        let key2 = HubSettingsApiKey2 {
            api_key: "abc".to_string(),
            domains: vec!["localhost".to_string(), "localhost3".to_string()],
        };
        assert_eq!(key1, key2);
    }

    #[test]
    fn partial_eq_works_with_no_domains_in_one_key() {
        let key1 = HubSettingsApiKey2 {
            api_key: "abc".to_string(),
            domains: vec!["localhost".to_string(), "localhost2".to_string()],
        };
        let key2 = HubSettingsApiKey2 {
            api_key: "abc".to_string(),
            domains: vec![],
        };
        assert_ne!(key1, key2);
    }

    #[test]
    fn partial_eq_works_with_same_domains3() {
        let key1 = HubSettingsApiKey2 {
            api_key: "abc".to_string(),
            domains: vec!["localhost3".to_string(), "localhost2".to_string()],
        };
        let key2 = HubSettingsApiKey2 {
            api_key: "abc".to_string(),
            domains: vec!["localhost4".to_string(), "localhost3".to_string()],
        };
        assert_eq!(key1, key2);
    }

    #[test]
    fn partial_eq_works_with_different_keys() {
        let key1 = HubSettingsApiKey2 {
            api_key: "abcd".to_string(),
            domains: vec!["localhost3".to_string(), "localhost2".to_string()],
        };
        let key2 = HubSettingsApiKey2 {
            api_key: "abc".to_string(),
            domains: vec!["localhost3".to_string(), "localhost2".to_string()],
        };
        assert_ne!(key1, key2);
    }

    #[test]
    fn partial_eq_works_with_same_domains4() {
        let key1 = HubSettingsApiKey2 {
            api_key: "abc".to_string(),
            domains: vec!["localhost3".to_string(), "localhost2".to_string()],
        };
        let key2 = HubSettingsApiKey2 {
            api_key: "abc".to_string(),
            domains: vec!["localhost2".to_string()],
        };
        assert_eq!(key1, key2);
    }

    #[test]
    fn partial_eq_works_with_same_domains5() {
        let key1 = HubSettingsApiKey2 {
            api_key: "abc".to_string(),
            domains: vec!["localhost2".to_string()],
        };
        let key2 = HubSettingsApiKey2 {
            api_key: "abc".to_string(),
            domains: vec!["localhost3".to_string(), "localhost2".to_string()],
        };
        assert_eq!(key1, key2);
    }
}
