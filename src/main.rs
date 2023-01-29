use std::env;
use fred::{prelude::*};

#[tokio::main]
async fn main() -> Result<(), RedisError> {
  let redis_address = env::var("REDIS_ADDRESS")
      .expect("REDIS_ADDRESS is not set");
  let redis_password = env::var("REDIS_PASSWORD")
      .expect("REDIS_PASSWORD is not set");
  let redis_sentinels = env::var("REDIS_SENTINELS")
      .expect("REDIS_SENTINELS is not set");

  println!("====== redis server {0}", redis_address);

  /*
  let redis_sentinel_servers: Vec<&str> = redis_sentinels.split(",")
      .collect();*/

  let sentinel_hosts = redis_sentinels
      .split(",")
      .map(|address| { address.split_once(":")
          .map(|(host, port)| {
            (host.to_string(), port.parse::<u16>().unwrap_or(0))
          }).unwrap() })
      .collect::<Vec<(String, u16)>>();

  println!("====================== sentinel servers {:?}", sentinel_hosts);

  let config = RedisConfig {
    server: ServerConfig::Sentinel {
      service_name: "mymaster".into(),
      hosts: sentinel_hosts,
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
