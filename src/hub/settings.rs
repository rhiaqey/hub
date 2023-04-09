use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HubSettingsApiKey {
    #[serde(alias = "Key")]
    pub api_key: String,
    #[serde(alias = "Domains")]
    pub domains: Vec<String>,
}

impl PartialEq for HubSettingsApiKey {
    fn eq(&self, other: &Self) -> bool {
        self.api_key == other.api_key
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct HubSettings {
    #[serde(alias = "ApiKeys")]
    pub api_keys: Vec<HubSettingsApiKey>,
}
