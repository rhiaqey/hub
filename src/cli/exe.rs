use crate::cli::security::generate_keys;
use crate::hub;
use clap::{arg, Command};
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
}

pub async fn run() {
    let matches = cli().get_matches();
    match matches.subcommand() {
        Some(("run", _sub_matches)) => {
            hub::exe::run().await;
        }
        Some(("generate-keys", sub_matches)) => {
            println!("generating keys");

            let (private_key, public_key) = generate_keys(None);

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
