use rhiaqey_common::stream::StreamMessage;
use rhiaqey_sdk::channel::Channel;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

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
            receiver: rx,
        }
    }

    pub fn start(&self) {
        tokio::spawn(async move {
            //
        });
    }

    pub fn get_name(&self) -> String {
        return self.channel.name.to_string();
    }
}
