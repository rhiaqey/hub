use crate::hub;

pub async fn run() -> anyhow::Result<()> {
    hub::exe::run().await
}
