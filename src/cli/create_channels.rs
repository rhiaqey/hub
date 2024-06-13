use crate::http::channels::create_channels;
use crate::http::state::CreateChannelsRequest;
use crate::hub;
use anyhow::bail;
use clap::ArgMatches;
use rhiaqey_sdk_rs::channel::ChannelList;
use std::fs;

pub async fn run(sub_matches: &ArgMatches) -> anyhow::Result<()> {
    if let Some(file) = sub_matches.get_one::<std::path::PathBuf>("file") {
        if !file.is_file() {
            panic!("could not find file: {:?}", file)
        }

        let path = file.canonicalize().unwrap().display().to_string();
        println!("load settings from file: {}", path);

        let contents = fs::read_to_string(path);
        if contents.is_err() {
            panic!("error reading from file: {}", contents.unwrap_err());
        }

        let data = contents.unwrap();
        let list: ChannelList = serde_json::from_str(data.as_str())?;

        let hub = hub::exe::create().await;
        println!("hub ready");

        let state = hub.create_shared_state();
        println!("hub state created");

        if let Err(err) = create_channels(CreateChannelsRequest { channels: list }, state).await {
            bail!("error creating channels: {}", err);
        }
    } else {
        bail!("required <FILE> is missing")
    }

    println!("creating channels finished successfully");

    Ok(())
}
