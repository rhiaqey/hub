use rhiaqey_hub::hub;

#[tokio::main]
async fn main() {
    hub::run::run().await;
}
