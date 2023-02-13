use crate::hub::channels::StreamingChannel;
use rustis::client::Client;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio_stream::StreamMap;

pub struct SharedState {
    pub namespace: String,
    pub redis: Arc<Mutex<Option<Client>>>,
    pub streams: Arc<RwLock<StreamMap<String, StreamingChannel>>>,
}
