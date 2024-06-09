use crate::cli::commands;
use clap::{arg, Command};

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
            commands::run::run().await;
        }
        Some(("load-settings", sub_matches)) => {
            commands::load_settings::run(sub_matches).await;
        }
        Some(("generate-keys", sub_matches)) => {
            commands::generate_keys::run(sub_matches).await;
        }
        _ => {
            println!("unknown command");
            unreachable!()
        }
    }
}
