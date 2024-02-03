use clap::{arg, Command};
use rsa::pkcs1::LineEnding;
use rsa::pkcs8::{EncodePrivateKey, EncodePublicKey};
use rsa::{RsaPrivateKey, RsaPublicKey};

fn cli() -> Command {
    Command::new("ops")
        .about("rhiaqey operations")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true)
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

fn generate_keys(bits: Option<usize>) -> (RsaPrivateKey, RsaPublicKey) {
    let mut rng = rand::thread_rng();
    let bits = bits.unwrap_or(2048);

    let pr = RsaPrivateKey::new(&mut rng, bits).expect("failed to generate a key");
    let pb = RsaPublicKey::from(&pr);

    (pr, pb)
}

fn main() {
    let matches = cli().get_matches();
    match matches.subcommand() {
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

                let dir = directory.to_str().unwrap();
                println!("storing generated keys in {dir}");

                let filename = directory.with_file_name("priv.pem");
                println!(
                    "checking if {} exists",
                    filename.canonicalize().unwrap().display()
                );

                let exists = filename.exists();
                println!(
                    "priv.pem file exists {} in {}",
                    exists,
                    directory.canonicalize().unwrap().display()
                );

                if *skip && exists {
                    println!("file already exist. skipping writing files to fs");
                } else {
                    private_key
                        .write_pkcs8_pem_file(format!("{dir}/priv.pem"), LineEnding::LF)
                        .expect("failed to write private pem file");

                    println!("private key was written");

                    public_key
                        .write_public_key_pem_file(format!("{dir}/pub.pem"), LineEnding::LF)
                        .expect("failed to write public pem file");

                    println!("public key was written");
                }
            }
        }
        _ => {
            println!("unknown command");
            unreachable!()
        }
    }
}
