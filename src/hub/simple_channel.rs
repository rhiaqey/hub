use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct SimpleChannel(String);

impl SimpleChannel {
    pub fn get_channel_with_category_and_key(&self) -> (String, Option<String>, Option<String>) {
        let parts: Vec<&str> = self.0.split('/').collect();
        match parts.len() {
            3 => (
                parts[0].to_string(),
                Some(parts[1].to_string()),
                Some(parts[2].to_string()),
            ),
            2 => (parts[0].to_string(), Some(parts[1].to_string()), None),
            _ => (parts[0].to_string(), None, None),
        }
    }
}

impl From<&str> for SimpleChannel {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl Display for SimpleChannel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct SimpleChannels(Vec<SimpleChannel>);

impl SimpleChannels {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get_channels_with_category_and_key(
        &self,
    ) -> Vec<(String, Option<String>, Option<String>)> {
        self.0
            .iter()
            .map(|x| x.get_channel_with_category_and_key())
            .collect()
    }
}

impl From<Vec<String>> for SimpleChannels {
    fn from(value: Vec<String>) -> Self {
        Self(
            value
                .iter()
                .map(|x| SimpleChannel { 0: x.clone() })
                .collect(),
        )
    }
}

impl From<Vec<&str>> for SimpleChannels {
    fn from(value: Vec<&str>) -> Self {
        Self {
            0: value.iter().map(|x| SimpleChannel::from(*x)).collect(),
        }
    }
}
