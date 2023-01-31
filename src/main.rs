use axum::routing::get;
use axum::Router;
use fred::prelude::*;
use std::env;
use std::net::SocketAddr;

use std::time::Duration;
use tokio::time::sleep;

// basic handler that responds with a static string
async fn root() -> &'static str {
    "Hello, World!"
}

const COUNT: usize = 60;

#[tokio::main]
async fn main() {
    let redis_address = env::var("REDIS_ADDRESS").expect("REDIS_ADDRESS is not set");
    let redis_password = env::var("REDIS_PASSWORD").expect("REDIS_PASSWORD is not set");
    let redis_sentinels = env::var("REDIS_SENTINELS").expect("REDIS_SENTINELS is not set");

    let sentinel_hosts = redis_sentinels
        .split(",")
        .map(|address| {
            address
                .split_once(":")
                .map(|(host, port)| (host.to_string(), port.parse::<u16>().unwrap_or(0)))
                .unwrap()
        })
        .collect::<Vec<(String, u16)>>();

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
    let publisher_client = RedisClient::new(config, None, Some(policy));
    // let publisher = subscriber.clone();

    let res = publisher_client.connect();
    println!("+++++++++++++++++++ connection result {:?}", res);
    let wait_result = publisher_client.wait_for_connect().await;
    println!("+++++++++++++++++++ wait result {:?}", wait_result);

    let subscriber_client = publisher_client.clone_new();

    let res = subscriber_client.connect();
    println!("+++++++++++++++++++ connection result {:?}", res);
    let wait_result = subscriber_client.wait_for_connect().await;
    println!("+++++++++++++++++++ wait result {:?}", wait_result);

    // spawn a task to manage subscription state automatically whenever the client reconnects
    // let _ = subscriber_client.manage_subscriptions();

    let s1: Result<RedisValue, RedisError> = subscriber_client.subscribe("foo").await;
    println!("foo sub {:?}", s1);
    if s1.is_err() {
        println!("we can do shit");
    }
    let s2: Result<RedisValue, RedisError> =
        subscriber_client.psubscribe(vec!["bar*", "baz*"]).await;
    println!("bar / baz subbed {:?}", s2);
    if s2.is_err() {
        println!("we can do shit part2");
    }

    /*
    println!(
        "Subscriber channels: {:?}",
        subscriber_client.tracked_channels()
    ); // "foo"
    println!(
        "Subscriber patterns: {:?}",
        subscriber_client.tracked_patterns()
    ); // "bar*", "baz*"
    println!(
        "Subscriber sharded channels: {:?}",
        subscriber_client.tracked_shard_channels()
    );*/

    tokio::spawn(async move {
        println!("subscriber spawned");
        let mut message_stream = subscriber_client.on_message();

        while let Ok(message) = message_stream.recv().await {
            println!(
                "------------ recv {:?} on channel {}",
                message.value, message.channel
            );
        }

        Ok::<_, RedisError>(())
    });

    println!("after spawning subscriber");

    tokio::spawn(async move {
        for idx in 0..COUNT {
            let _ = publisher_client.publish("foo", idx).await?;
            println!("published message foo:{}", idx);
            // sleep(Duration::from_millis(100)).await;
        }

        sleep(Duration::from_millis(150)).await;

        for idx in 0..(COUNT * 2) {
            println!("published message bar:{}", idx);
            let _ = publisher_client
                .publish(format!("bar{0}", idx), idx)
                .await?;
            // sleep(Duration::from_millis(300)).await;
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
