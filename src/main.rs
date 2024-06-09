pub(crate) mod cli;
pub(crate) mod http;
pub(crate) mod hub;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    cli::run().await
}
