use axum::extract::ws::Message;
use rhiaqey_sdk_rs::channel::Channel;
use websocket::WebSocketClient;

pub(crate) mod websocket;

pub(crate) enum HubClient {
    WebSocket(WebSocketClient),
}

impl HubClient {
    pub fn get_hub_id(&self) -> &String {
        match self {
            HubClient::WebSocket(c) => c.get_hub_id(),
        }
    }

    pub fn get_client_id(&self) -> &String {
        match self {
            HubClient::WebSocket(c) => c.get_client_id(),
        }
    }

    pub fn get_user_id(&self) -> &Option<String> {
        match self {
            HubClient::WebSocket(c) => c.get_user_id(),
        }
    }

    pub async fn send(&mut self, data: Vec<u8>) -> anyhow::Result<()> {
        match self {
            HubClient::WebSocket(c) => c.send(Message::Binary(data)).await,
        }
    }

    pub fn get_category_for_channel(
        &self,
        name: &String,
    ) -> Option<(&Option<String>, &Option<String>)> {
        match self {
            HubClient::WebSocket(c) => c.get_category_for_channel(name),
        }
    }

    pub fn get_channels(&self) -> &Vec<(Channel, Option<String>, Option<String>)> {
        match self {
            HubClient::WebSocket(c) => &c.channels
        }
    }
}
