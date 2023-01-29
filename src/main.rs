use std::env;
use std::net::SocketAddr;
use axum::Router;
use axum::routing::get;
use fred::{prelude::*};

use tokio::time::sleep;
use std::time::Duration;

// basic handler that responds with a static string
async fn root() -> &'static str {
  "Hello, World!"
}

const COUNT: usize = 60;

#[tokio::main]
async fn main() {
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
  let subscriber = RedisClient::new(config, None, Some(policy));
  let publisher = subscriber.clone();

  let res = subscriber.connect();
  println!("+++++++++++++++++++ connection result {:?}", res);
  let wait_result = subscriber.wait_for_connect().await.unwrap();
  println!("+++++++++++++++++++ wait result {:?}", wait_result);

  let _: Result<String, RedisError> = subscriber.subscribe("foo").await;
  println!("foo sub");
  let _: Result<String, RedisError> = subscriber.psubscribe(vec!["bar*", "baz*"]).await;
  println!("bar / baz subbed");

  tokio::spawn(async move {
    println!("subscriber spawned");
    let mut message_stream = subscriber.on_message();

    while let Ok(message) = message_stream.recv().await {
      println!("------------ recv {:?} on channel {}", message.value, message.channel);
    }

    Ok::<_, RedisError>(())
  });

  println!("after spawning subscriber");

  tokio::spawn(async move {
    for idx in 0..COUNT {
      println!("published message foo:{}", idx);
      let _ = publisher.publish("foo", idx).await?;
      sleep(Duration::from_millis(1000)).await;
    }

    sleep(Duration::from_millis(15000)).await;

    for idx in 0..(COUNT * 2) {
      println!("published message bar:{}", idx);
      let _ = publisher.publish("bar", idx).await?;
      sleep(Duration::from_millis(3000)).await;
    }

    Ok::<_, RedisError>(())
  });

  // build our application with a route
  let app = Router::new()
      // `GET /` goes to `root`
      .route("/alive", get(root));

  // run our app with hyper
  // `axum::Server` is a re-export of `hyper::Server`
  let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
  println!("listening on {}", addr);

  axum::Server::bind(&addr)
      .serve(app.into_make_service())
      .await
      .unwrap()
}
