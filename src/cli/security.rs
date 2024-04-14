use rsa::{RsaPrivateKey, RsaPublicKey};

pub fn generate_keys(bits: Option<usize>) -> anyhow::Result<(RsaPrivateKey, RsaPublicKey)> {
    let mut rng = rand::thread_rng();
    let bits = bits.unwrap_or(2048);

    let pr = RsaPrivateKey::new(&mut rng, bits)?;
    let pb = RsaPublicKey::from(&pr);

    Ok((pr, pb))
}
