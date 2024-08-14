use axum::extract::ws::Message;
use rhiaqey_common::client::ClientMessage;
use rhiaqey_sdk_rs::channel::Channel;
use sse::SSEClient;
use websocket::WebSocketClient;

pub(crate) mod websocket;
pub(crate) mod sse;

pub(crate) enum HubClient {
    SSE(SSEClient),
    WebSocket(WebSocketClient),
}

impl HubClient {
    pub fn get_hub_id(&self) -> &String {
        match self {
            HubClient::SSE(c) => c.get_hub_id(),
            HubClient::WebSocket(c) => c.get_hub_id(),
        }
    }

    pub fn get_client_id(&self) -> &String {
        match self {
            HubClient::SSE(c) => c.get_client_id(),
            HubClient::WebSocket(c) => c.get_client_id(),
        }
    }

    pub fn get_user_id(&self) -> &Option<String> {
        match self {
            HubClient::SSE(c) => c.get_user_id(),
            HubClient::WebSocket(c) => c.get_user_id(),
        }
    }

    pub async fn send_raw(&mut self, data: Vec<u8>) -> anyhow::Result<()> {
        match self {
            HubClient::SSE(c) => c.send(String::from_utf8(data)?).await,
            HubClient::WebSocket(c) => c.send(Message::Binary(data)).await,
        }
    }

    pub async fn send_message(&mut self, data: &ClientMessage) -> anyhow::Result<()> {
        match self {
            HubClient::SSE(c) => {
                let raw = data.ser_to_json()?;
                c.send(String::from_utf8(raw)?).await
            }
            HubClient::WebSocket(c) => {
                let raw = data.ser_to_msgpack()?;
                c.send(Message::Binary(raw)).await
            }
        }
    }

    pub fn get_category_for_channel(
        &self,
        name: &String,
    ) -> Option<(&Option<String>, &Option<String>)> {
        match self {
            HubClient::SSE(c) => c.get_category_for_channel(name),
            HubClient::WebSocket(c) => c.get_category_for_channel(name),
        }
    }

    pub fn get_channels(&self) -> &Vec<(Channel, Option<String>, Option<String>)> {
        match self {
            HubClient::SSE(c) => &c.channels,
            HubClient::WebSocket(c) => &c.channels
        }
    }
}
