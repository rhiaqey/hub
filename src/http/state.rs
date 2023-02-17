use crate::hub::channels::StreamingChannel;
use rhiaqey_common::env::Env;
use rustis::client::Client;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio_stream::StreamMap;

pub struct SharedState {
    pub env: Arc<Env>,
    pub redis: Arc<Mutex<Option<Client>>>,
    pub streams: Arc<RwLock<StreamMap<String, StreamingChannel>>>,
}

impl SharedState {
    pub fn get_namespace(&self) -> String {
        return self.env.namespace.clone();
    }
}
