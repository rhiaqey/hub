use anyhow::bail;
use clap::ArgMatches;
use rsa::pkcs1::{EncodeRsaPrivateKey, EncodeRsaPublicKey, LineEnding};
use rsa::{RsaPrivateKey, RsaPublicKey};
use std::fs;

fn generate_keys(bits: Option<usize>) -> anyhow::Result<(RsaPrivateKey, RsaPublicKey)> {
    let mut rng = rand::thread_rng();
    let bits = bits.unwrap_or(2048);

    let pr = RsaPrivateKey::new(&mut rng, bits)?;
    let pb = RsaPublicKey::from(&pr);

    Ok((pr, pb))
}

pub async fn run(sub_matches: &ArgMatches) -> anyhow::Result<()> {
    println!("generating keys");

    let (private_key, public_key) = generate_keys(None).expect("failed to generate security keys");

    let skip = sub_matches.get_one::<bool>("skip").unwrap_or(&false);
    if *skip {
        println!("skip flag is set")
    }

    if let Some(directory) = sub_matches.get_one::<std::path::PathBuf>("write") {
        if directory.is_dir() {
            println!("directory found");
        } else {
            bail!("{} is not a valid directory", directory.to_str().unwrap());
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

    Ok(())
}
