use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HubSettingsApiKey {
    #[serde(alias = "Key")]
    pub key: String,
}

impl PartialEq for HubSettingsApiKey {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HubSettings {
    #[serde(alias = "ApiKeys")]
    pub api_keys: Vec<HubSettingsApiKey>,
}