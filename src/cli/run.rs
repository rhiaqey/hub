use crate::hub;

pub async fn run() -> anyhow::Result<()> {
    let mut hub = hub::exe::create().await?;
    println!("hub ready");

    hub.start().await
}
