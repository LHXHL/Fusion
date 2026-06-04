use aes_gcm_siv::{
    aead::{generic_array::GenericArray, Aead, KeyInit, Result},
    Aes256GcmSiv, Nonce,
};
use data_encoding::HEXLOWER;
use std::io::{Error, ErrorKind};

use crate::utils::random::random_string;

pub const AES_GCM_KEY_LENGTH: usize = 32;
pub const AES_GCM_NONCE_LENGTH: usize = 12;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncMessage {
    pub ciphertext: String,
    pub nonce: String,
}

pub fn string_to_u8_12(text: &str) -> core::result::Result<[u8; 12], Error> {
    if text.len() != 12 {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "Input string length is not 12.",
        ));
    }

    let mut bytearray: [u8; 12] = [0; 12];
    bytearray[..text.len()].copy_from_slice(&text.as_bytes()[..12]);
    Ok(bytearray)
}

pub fn vec_u8_to_u8_32(v: Vec<u8>) -> core::result::Result<[u8; 32], Error> {
    let bytearray: [u8; 32] = v
        .try_into()
        .map_err(|e| Error::new(ErrorKind::Other, format!("Error: {:?}", e)))?;
    Ok(bytearray)
}

pub fn encrypt(plaintext: &[u8], key: &[u8], nonce: &[u8]) -> Result<Vec<u8>> {
    let key = GenericArray::from_slice(key);
    let nonce = Nonce::from_slice(nonce);

    let cipher = Aes256GcmSiv::new(key);
    cipher.encrypt(nonce, plaintext.as_ref())
}

pub fn decrypt(ciphertext: &[u8], key: &[u8], nonce: &[u8]) -> Result<Vec<u8>> {
    let key = GenericArray::from_slice(key);
    let nonce = Nonce::from_slice(nonce);

    let cipher = Aes256GcmSiv::new(key);
    cipher.decrypt(nonce, ciphertext.as_ref())
}

pub fn encode(input: &[u8]) -> String {
    HEXLOWER.encode(input)
}

pub fn decode(input: &[u8]) -> Vec<u8> {
    HEXLOWER.decode(input.as_ref()).unwrap()
}

pub fn seal(plaintext: &[u8], key: &[u8]) -> Result<EncMessage> {
    let nonce = random_string(AES_GCM_NONCE_LENGTH);
    let encrypted = encrypt(plaintext, key, nonce.as_bytes())?;

    Ok(EncMessage {
        ciphertext: encode(&encrypted),
        nonce,
    })
}

pub fn open(message: &EncMessage, key: &[u8]) -> Result<Vec<u8>> {
    let decoded_ciphertext = decode(message.ciphertext.as_bytes());
    decrypt(&decoded_ciphertext, key, message.nonce.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::{open, seal, AES_GCM_KEY_LENGTH};

    #[test]
    fn seal_open_roundtrip() {
        let key = vec![7_u8; AES_GCM_KEY_LENGTH];
        let msg = seal(b"fusion", &key).unwrap();
        let plain = open(&msg, &key).unwrap();
        assert_eq!(plain, b"fusion");
    }
}
