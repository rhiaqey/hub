use crate::hub::channels::StreamingChannel;
use rhiaqey_common::env::Env;
use rustis::client::Client;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;

pub struct SharedState {
    pub env: Arc<Env>,
    pub redis: Arc<Mutex<Option<Client>>>,
    pub sender: UnboundedSender<u128>,
    pub streams: Arc<Mutex<HashMap<String, StreamingChannel>>>,
}

impl SharedState {
    pub fn get_namespace(&self) -> String {
        return self.env.namespace.clone();
    }
}
