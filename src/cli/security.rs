use rsa::{RsaPrivateKey, RsaPublicKey};

pub fn generate_keys(bits: Option<usize>) -> (RsaPrivateKey, RsaPublicKey) {
    let mut rng = rand::thread_rng();
    let bits = bits.unwrap_or(2048);

    let pr = RsaPrivateKey::new(&mut rng, bits).expect("failed to generate a key");
    let pb = RsaPublicKey::from(&pr);

    (pr, pb)
}
