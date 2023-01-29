use std::env;
use fred::{prelude::*};

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let redis_password = env::var("REDIS_PASSWORD")
      .expect("REDIS_PASSWORD is not set");

  let config = RedisConfig {
    server: ServerConfig::Sentinel {
      service_name: "mymaster".into(),
      hosts: vec![
        ("localhost".into(), 26379),
      ],
    },
    password: Some(redis_password.to_string()),
    ..Default::default()
  };

  println!("===== CLIENT config {:?}", config);

  let policy = ReconnectPolicy::default();
  let client = RedisClient::new(config, None, Some(policy));

  let _ = client.connect();
  let _ = client.wait_for_connect().await?;

  // do stuff

  let _ = client.quit().await?;
  Ok(())
}
