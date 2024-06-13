use anyhow::bail;
use clap::ArgMatches;
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

        // TODO
    } else {
        bail!("required <FILE> is missing")
    }

    Ok(())
}
