mod hub;
mod http;

#[tokio::main]
async fn main() {
    hub::run().await
}
