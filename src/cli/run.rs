use crate::hub;

pub async fn run() -> anyhow::Result<()> {
    let mut hub = hub::exe::create().await?;
    hub.start().await
}
