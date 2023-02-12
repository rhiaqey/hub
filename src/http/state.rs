use rustis::client::Client;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct SharedState {
    pub namespace: String,
    pub redis: Arc<Mutex<Option<Client>>>,
}
