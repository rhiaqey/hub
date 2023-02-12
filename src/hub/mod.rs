use crate::http::start_http_server;
use crate::http::state::SharedState;
use log::debug;
use rhiaqey_common::env::{parse_env, Env};
use rhiaqey_common::redis;
use rustis::client::Client;
use rustis::commands::{ConnectionCommands, PingOptions};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct Hub {
    env: Arc<Env>,
    redis: Arc<Mutex<Option<Client>>>,
}

impl Hub {
    pub fn get_id(&self) -> String {
        self.env.id.clone()
    }

    pub fn get_name(&self) -> String {
        self.env.name.clone()
    }

    pub fn is_debug(&self) -> bool {
        self.env.debug
    }

    pub fn get_private_port(&self) -> u16 {
        self.env.private_port
    }

    pub async fn setup(config: Env) -> Result<Hub, String> {
        let redis_connection = redis::connect(config.redis.clone()).await;
        let result: String = redis_connection
            .clone()
            .unwrap()
            .ping(PingOptions::default().message("hello"))
            .await
            .unwrap();
        if result != "hello" {
            return Err("ping failed".to_string());
        }

        Ok(Hub {
            env: Arc::from(config),
            redis: Arc::new(Mutex::new(redis_connection)),
        })
    }

    pub async fn start(&self) -> hyper::Result<()> {
        let port = self.get_private_port();

        let shared_state = Arc::new(SharedState {
            namespace: self.env.namespace.clone(),
            redis: self.redis.clone(),
        });

        start_http_server(port, shared_state).await
    }
}

pub async fn run() {
    env_logger::init();
    let env = parse_env();

    let hub = match Hub::setup(env).await {
        Ok(exec) => exec,
        Err(error) => {
            panic!("failed to setup hub: {error}");
        }
    };

    debug!(
        "hub [id={},name={},debug={}] is ready",
        hub.get_id(),
        hub.get_name(),
        hub.is_debug()
    );

    hub.start().await.unwrap()
}
