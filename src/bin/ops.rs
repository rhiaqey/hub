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

            if let Some(directory) = sub_matches.get_one::<std::path::PathBuf>("write") {
                if !directory.is_dir() {
                    panic!("{} is not a valid directory", directory.to_str().unwrap());
                }

                let dir = directory.to_str().unwrap();

                println!("storing generated keys in {dir}");

                private_key
                    .write_pkcs8_pem_file(format!("{dir}/priv.pem"), LineEnding::LF)
                    .expect("failed to write private pem file");

                public_key
                    .write_public_key_pem_file(format!("{dir}/pub.pem"), LineEnding::LF)
                    .expect("failed to write public pem file")
            }
        }
        _ => unreachable!(), // If all subcommands are defined above, anything else is unreachable!()
    }
}
