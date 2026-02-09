use crate::http::settings::{update_settings_for_hub, update_settings_for_publishers};
use crate::http::state::UpdateSettingsRequest;
use crate::hub;
use anyhow::bail;
use clap::ArgMatches;
use rhiaqey_sdk_rs::message::MessageValue;
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

        let empty = String::from("");
        let name = sub_matches.get_one::<String>("name").unwrap_or(&empty);
        if name.is_empty() {
            panic!("required <NAME> argument")
        }

        println!("name found: {}", name);

        let hub = hub::exe::create().await?;
        println!("hub ready");

        let state = hub.create_shared_state();
        println!("hub state created");

        if state.get_name() == name.to_string() {
            update_settings_for_hub(
                UpdateSettingsRequest {
                    name: name.to_string(),
                    settings: MessageValue::Text(data),
                },
                state,
            )
            .await?;
        } else {
            update_settings_for_publishers(
                UpdateSettingsRequest {
                    name: name.to_string(),
                    settings: MessageValue::Text(data),
                },
                state,
            )?;
        }
    } else {
        bail!("required <FILE> is missing")
    }

    println!("loading settings finished successfully");

    Ok(())
}
