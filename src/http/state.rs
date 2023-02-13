use rhiaqey_common::stream::StreamMessage;
use rhiaqey_sdk::channel::Channel;
use rustis::client::Client;
use tokio_stream::StreamMap;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc::{UnboundedSender, UnboundedReceiver, unbounded_channel}, RwLock};

pub struct StreamingChannel {
    pub channel: Channel,
    pub sender: UnboundedSender<StreamMessage>,
    pub receiver: UnboundedReceiver<StreamMessage>,
}

impl StreamingChannel {
    pub fn new(channel: Channel) -> StreamingChannel {
        let (tx, rx) = unbounded_channel::<StreamMessage>();
        StreamingChannel {
            channel,
            sender: tx,
            receiver: rx
        }
    }

    pub fn get_id(&self) -> String {
        return self.channel.name.to_string();
    }
}


pub struct SharedState {
    pub namespace: String,
    pub redis: Arc<Mutex<Option<Client>>>,
    pub streams: Arc<RwLock<StreamMap<String, StreamingChannel>>>
}
