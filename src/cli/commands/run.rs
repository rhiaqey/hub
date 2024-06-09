use crate::hub;

pub async fn run() {
    hub::exe::run().await;
}
