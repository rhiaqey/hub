use crate::cli::security::generate_keys;
use crate::http::settings::update_settings_for_hub;
use crate::http::state::UpdateSettingsRequest;
use crate::hub;
use clap::{arg, Command};
use rhiaqey_sdk_rs::message::MessageValue;
use rsa::pkcs1::{EncodeRsaPrivateKey, EncodeRsaPublicKey, LineEnding};
use std::fs;

fn cli() -> Command {
    Command::new("hub")
        .about("rhiaqey hub")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
        .subcommand(Command::new("run").about("Run hub"))
        .subcommand(
            Command::new("generate-keys")
                .about("Generate RSA keys")
                .arg(
                    arg!(-w --write <DIR>)
                        .value_parser(clap::value_parser!(std::path::PathBuf))
                        .required(false),
                )
                .arg(
                    arg!(-s - -skip)
                        .value_parser(clap::value_parser!(bool))
                        .required(false),
                ),
        )
        .subcommand(
            Command::new("load-settings")
                .about("Load settings from json")
                .arg(
                    arg!(-f --file <FILE>)
                        .value_parser(clap::value_parser!(std::path::PathBuf))
                        .required(true),
                )
                .arg(
                    arg!(-n --name <NAME>)
                        .value_parser(clap::value_parser!(String))
                        .required(true),
                ),
        )
}

pub async fn run() {
    let matches = cli().get_matches();
    match matches.subcommand() {
        Some(("run", _sub_matches)) => {
            hub::exe::run().await;
        }
        Some(("load-settings", sub_matches)) => {
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

                let hub = hub::exe::create().await;
                println!("hub ready");

                let _state = hub.create_shared_state();
                println!("hub state created");

                update_settings_for_hub(
                    UpdateSettingsRequest {
                        name: name.to_string(),
                        settings: MessageValue::Text(data),
                    },
                    _state,
                )
                .expect("failed to update settings for hub");
            } else {
                panic!("required <FILE> is missing")
            }
        }
        Some(("generate-keys", sub_matches)) => {
            println!("generating keys");

            let (private_key, public_key) =
                generate_keys(None).expect("failed to generate security keys");

            let skip = sub_matches.get_one::<bool>("skip").unwrap_or(&false);
            if *skip {
                println!("skip flag is set")
            }

            if let Some(directory) = sub_matches.get_one::<std::path::PathBuf>("write") {
                if directory.is_dir() {
                    println!("directory found");
                } else {
                    panic!("{} is not a valid directory", directory.to_str().unwrap());
                }

                let dir = directory.canonicalize().unwrap().display().to_string();
                println!("storing generated keys in {dir}");

                let mut exists = false;

                let contents = fs::read_to_string(format!("{}/priv.pem", dir));
                if contents.is_err() {
                    println!("error reading content from directory {}/priv.pem", dir)
                } else {
                    exists = true;
                    println!("file was read successfully");
                }

                if *skip && exists {
                    println!("file already exist. skipping writing files to fs");
                } else {
                    println!("writing pem files");

                    private_key
                        .write_pkcs1_pem_file(format!("{dir}/priv.pem"), LineEnding::LF)
                        .expect("failed to write private pem file");

                    println!("private key was written");

                    public_key
                        .write_pkcs1_pem_file(format!("{dir}/pub.pem"), LineEnding::LF)
                        .expect("failed to write public pem file");

                    println!("public key was written");
                }
            } else {
                println!("nothing to write though");
            }
        }
        _ => {
            println!("unknown command");
            unreachable!()
        }
    }
}
