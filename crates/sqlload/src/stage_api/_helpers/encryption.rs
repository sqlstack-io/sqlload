#[cfg(test)]
use aes::cipher::BlockDecrypt;
use aes::cipher::{BlockEncrypt, KeyInit};
use aes::{Aes128, Aes192, Aes256};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use crate::stage_api::models::{EncryptionMaterial, EncryptionMetadata};

const BLOCK: usize = 16;

enum Cipher {
    A128(Box<Aes128>),
    A192(Box<Aes192>),
    A256(Box<Aes256>),
}

impl Cipher {
    fn new(key: &[u8]) -> Result<Self, String> {
        match key.len() {
            16 => Aes128::new_from_slice(key).map(|cipher| Self::A128(Box::new(cipher))),
            24 => Aes192::new_from_slice(key).map(|cipher| Self::A192(Box::new(cipher))),
            32 => Aes256::new_from_slice(key).map(|cipher| Self::A256(Box::new(cipher))),
            length => return Err(format!("unsupported AES key length {length}")),
        }
        .map_err(|error| error.to_string())
    }

    fn encrypt_block(&self, block: &mut [u8; BLOCK]) {
        let block = block.into();
        match self {
            Self::A128(cipher) => cipher.encrypt_block(block),
            Self::A192(cipher) => cipher.encrypt_block(block),
            Self::A256(cipher) => cipher.encrypt_block(block),
        }
    }

    #[cfg(test)]
    fn decrypt_block(&self, block: &mut [u8; BLOCK]) {
        let block = block.into();
        match self {
            Self::A128(cipher) => cipher.decrypt_block(block),
            Self::A192(cipher) => cipher.decrypt_block(block),
            Self::A256(cipher) => cipher.decrypt_block(block),
        }
    }
}

fn pad(data: &[u8]) -> Vec<u8> {
    let padding = BLOCK - data.len() % BLOCK;
    let mut output = Vec::with_capacity(data.len() + padding);
    output.extend_from_slice(data);
    output.resize(data.len() + padding, padding as u8);
    output
}

#[cfg(test)]
fn unpad(data: &[u8]) -> Result<&[u8], String> {
    let padding = usize::from(*data.last().ok_or("empty ciphertext")?);
    let valid = (1..=BLOCK).contains(&padding)
        && padding <= data.len()
        && data[data.len() - padding..]
            .iter()
            .all(|byte| usize::from(*byte) == padding);
    match valid {
        true => Ok(&data[..data.len() - padding]),
        false => Err("bad padding".to_owned()),
    }
}

fn random_bytes(length: usize) -> Result<Vec<u8>, String> {
    let mut buffer = vec![0u8; length];
    getrandom::fill(&mut buffer)
        .map_err(|error| format!("random number generator failed: {error}"))?;
    Ok(buffer)
}

/// AES-CBC encrypt `plaintext` with a random file key, the file key AES-ECB encrypted with the stage master key.
pub(crate) fn encrypt(
    material: &EncryptionMaterial,
    plaintext: &[u8],
) -> Result<(Vec<u8>, EncryptionMetadata), String> {
    let master = STANDARD
        .decode(material.query_stage_master_key.expose())
        .map_err(|error| format!("bad stage master key: {error}"))?;
    let file_key = random_bytes(master.len())?;
    let iv = random_bytes(BLOCK)?;
    let file_cipher = Cipher::new(&file_key)?;
    let mut ciphertext = pad(plaintext);
    let mut previous = [0u8; BLOCK];
    previous.copy_from_slice(&iv);
    for block in ciphertext.as_chunks_mut::<BLOCK>().0.iter_mut() {
        block
            .iter_mut()
            .zip(previous.iter())
            .for_each(|(byte, prior)| *byte ^= prior);
        file_cipher.encrypt_block(block);
        previous.copy_from_slice(block);
    }
    let master_cipher = Cipher::new(&master)?;
    let mut wrapped = pad(&file_key);
    wrapped
        .as_chunks_mut::<BLOCK>()
        .0
        .iter_mut()
        .for_each(|block| master_cipher.encrypt_block(block));
    let metadata = EncryptionMetadata {
        key: STANDARD.encode(wrapped),
        iv: STANDARD.encode(iv),
        matdesc: format!(
            "{{\"queryId\":{},\"smkId\":\"{}\",\"keySize\":\"{}\"}}",
            serde_json::Value::String(material.query_id.clone()),
            material.smk_id,
            master.len() * 8
        ),
    };
    Ok((ciphertext, metadata))
}

/// The inverse of [`encrypt`].
#[cfg(test)]
pub(crate) fn decrypt(
    material: &EncryptionMaterial,
    metadata: &EncryptionMetadata,
    ciphertext: &[u8],
) -> Result<Vec<u8>, String> {
    let decode = |text: &str| STANDARD.decode(text).map_err(|error| error.to_string());
    let master_cipher = Cipher::new(&decode(material.query_stage_master_key.expose())?)?;
    let mut wrapped = decode(&metadata.key)?;
    if wrapped.is_empty() || wrapped.len() % BLOCK != 0 {
        return Err("bad wrapped key length".to_owned());
    }
    wrapped
        .as_chunks_mut::<BLOCK>()
        .0
        .iter_mut()
        .for_each(|block| master_cipher.decrypt_block(block));
    let file_cipher = Cipher::new(unpad(&wrapped)?)?;
    let iv = decode(&metadata.iv)?;
    if iv.len() != BLOCK || ciphertext.is_empty() || !ciphertext.len().is_multiple_of(BLOCK) {
        return Err("bad IV or ciphertext length".to_owned());
    }
    let mut plaintext = ciphertext.to_vec();
    let mut previous = [0u8; BLOCK];
    previous.copy_from_slice(&iv);
    for block in plaintext.as_chunks_mut::<BLOCK>().0.iter_mut() {
        let mut current = [0u8; BLOCK];
        current.copy_from_slice(block);
        file_cipher.decrypt_block(block);
        block
            .iter_mut()
            .zip(previous.iter())
            .for_each(|(byte, prior)| *byte ^= prior);
        previous = current;
    }
    Ok(unpad(&plaintext)?.to_vec())
}
