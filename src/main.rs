mod cli;
mod http;
mod hub;

#[tokio::main]
async fn main() {
    cli::exe::run().await;
}
