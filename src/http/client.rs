use axum::extract::ws::{CloseFrame, Message, WebSocket};
use axum::Error;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use log::warn;
use std::borrow::Cow;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

pub struct WebSocketClient {
    id: Uuid,
    sender: Arc<Mutex<SplitSink<WebSocket, Message>>>,
    receiver: Arc<Mutex<SplitStream<WebSocket>>>,
    join_handler: Option<Arc<JoinHandle<u32>>>,
}

impl WebSocketClient {
    pub fn create(client_id: Uuid, socket: WebSocket) -> WebSocketClient {
        let (sender, receiver) = socket.split();
        WebSocketClient {
            id: client_id,
            sender: Arc::new(Mutex::new(sender)),
            receiver: Arc::new(Mutex::new(receiver)),
            join_handler: None,
        }
    }

    pub fn get_id(&self) -> Uuid {
        self.id
    }

    pub fn listen(&mut self) {
        let sx = self.sender.clone();
        let rx = self.receiver.clone();

        let join_handler = tokio::task::spawn(async move {
            loop {
                while let Some(Ok(_msg)) = rx.lock().await.next().await {
                    warn!("received data from client");
                    warn!("closing connection");
                    if let Err(e) = sx
                        .lock()
                        .await
                        .send(Message::Close(Some(CloseFrame {
                            code: axum::extract::ws::close_code::NORMAL,
                            reason: Cow::from("invalid body"),
                        })))
                        .await
                    {
                        warn!("could not send Close due to {}, probably it is ok?", e);
                    } else {
                        sx.lock().await.close().await.unwrap()
                    }

                    break;
                }
            }
        });

        self.join_handler = Some(Arc::new(join_handler));
    }

    pub async fn send(&mut self, message: Message) -> Result<(), Error> {
        self.sender.lock().await.send(message).await
    }

    pub async fn close(&mut self) -> Result<(), Error> {
        self.sender.lock().await.close().await
    }
}

impl Drop for WebSocketClient {
    fn drop(&mut self) {
        if self.join_handler.is_some() {
            self.join_handler.as_mut().unwrap().abort();
        }
    }
}
