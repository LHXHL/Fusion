use aes_gcm_siv::aead::OsRng;
use x25519_dalek::{PublicKey, SharedSecret, StaticSecret};

pub fn generate_keypair() -> (StaticSecret, PublicKey) {
    let secret_key = StaticSecret::random_from_rng(OsRng);
    let public_key = PublicKey::from(&secret_key);
    (secret_key, public_key)
}

pub fn derive_shared_secret(my_secret: &StaticSecret, peer_public: &PublicKey) -> SharedSecret {
    my_secret.diffie_hellman(peer_public)
}

#[cfg(test)]
mod tests {
    use super::{derive_shared_secret, generate_keypair};

    #[test]
    fn key_exchange_roundtrip_matches() {
        let (a_secret, a_public) = generate_keypair();
        let (b_secret, b_public) = generate_keypair();

        let shared_a = derive_shared_secret(&a_secret, &b_public);
        let shared_b = derive_shared_secret(&b_secret, &a_public);

        assert_eq!(shared_a.as_bytes(), shared_b.as_bytes());
    }
}
