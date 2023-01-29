use fred::{prelude::*};
use fred::types::RespVersion::RESP3;

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let config = RedisConfig {
    server: ServerConfig::Sentinel {
      service_name: "mymaster".into(),
      hosts: vec![
        ("localhost".into(), 26379),
      ],
    },
    password: Some("".to_string()),
    version: RESP3,
    ..Default::default()
  };

  let policy = ReconnectPolicy::default();
  let client = RedisClient::new(config, None, Some(policy));
  let _ = client.connect();
  let _ = client.wait_for_connect().await?;

  // do stuff

  let _ = client.quit().await?;
  Ok(())
}