use rhiaqey_common::client::{
    ClientMessage, ClientMessageDataType, ClientMessageValue, ClientMessageValueClientConnection,
};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotParam {
    ASC,
    DESC,
    TRUE,
    FALSE,
}

impl Default for SnapshotParam {
    fn default() -> Self {
        Self::FALSE
    }
}

impl SnapshotParam {
    pub fn allowed(&self) -> bool {
        match *self {
            SnapshotParam::ASC => true,
            SnapshotParam::DESC => true,
            SnapshotParam::TRUE => true,
            SnapshotParam::FALSE => false,
        }
    }
}

#[derive(Deserialize)]
pub struct Params {
    pub channels: String,
    pub snapshot: Option<SnapshotParam>,
    pub snapshot_size: Option<usize>,
    pub user_id: Option<String>,
}

#[inline(always)]
pub fn prepare_client_connection_message(
    client_id: &String,
    hub_id: &String,
) -> anyhow::Result<ClientMessage> {
    let mut client_message = ClientMessage {
        data_type: ClientMessageDataType::ClientConnection as u8,
        channel: String::from(""),
        key: String::from(""),
        value: ClientMessageValue::ClientConnection(ClientMessageValueClientConnection {
            client_id: client_id.to_string(),
            hub_id: hub_id.to_string(),
        }),
        tag: None,
        category: None,
        hub_id: None,
        publisher_id: None,
    };

    if cfg!(debug_assertions) {
        client_message.hub_id = Some(hub_id.clone());
    }

    Ok(client_message)
}
