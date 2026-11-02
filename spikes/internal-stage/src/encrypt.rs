//! Client-side encryption for Snowflake internal stages on S3.
//!
//! Mirrors the official drivers (`encryption_util.py` / `encrypt_util.go`):
//! - a random file key (same length as the query stage master key) and a random 16-byte IV;
//! - the file is AES-CBC encrypted with the file key, PKCS#7 padded;
//! - the file key is PKCS#7 padded and AES-ECB encrypted with the master key;
//! - the metadata travels as S3 user metadata: `x-amz-key` (encrypted file key, base64),
//!   `x-amz-iv` (base64) and `x-amz-matdesc` (JSON with queryId, smkId and keySize in bits).

use aes::cipher::{BlockDecrypt, BlockEncrypt, KeyInit, generic_array::GenericArray};
use aes::{Aes128, Aes192, Aes256};
use anyhow::{Result, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;

use crate::util::random_bytes;

const BLOCK: usize = 16;

#[derive(Clone, Debug)]
pub struct EncryptionMaterial {
    /// Base64 query stage master key.
    pub query_stage_master_key: String,
    pub query_id: String,
    pub smk_id: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncryptionMetadata {
    pub key: String,
    pub iv: String,
    pub matdesc: String,
}

enum Cipher {
    A128(Box<Aes128>),
    A192(Box<Aes192>),
    A256(Box<Aes256>),
}

impl Cipher {
    fn new(key: &[u8]) -> Result<Self> {
        Ok(match key.len() {
            16 => Cipher::A128(Box::new(Aes128::new(GenericArray::from_slice(key)))),
            24 => Cipher::A192(Box::new(Aes192::new(GenericArray::from_slice(key)))),
            32 => Cipher::A256(Box::new(Aes256::new(GenericArray::from_slice(key)))),
            n => bail!("unsupported AES key length {n}"),
        })
    }

    fn encrypt_block(&self, block: &mut [u8]) {
        let b = GenericArray::from_mut_slice(block);
        match self {
            Cipher::A128(c) => c.encrypt_block(b),
            Cipher::A192(c) => c.encrypt_block(b),
            Cipher::A256(c) => c.encrypt_block(b),
        }
    }

    fn decrypt_block(&self, block: &mut [u8]) {
        let b = GenericArray::from_mut_slice(block);
        match self {
            Cipher::A128(c) => c.decrypt_block(b),
            Cipher::A192(c) => c.decrypt_block(b),
            Cipher::A256(c) => c.decrypt_block(b),
        }
    }
}

fn pkcs7_pad(data: &[u8]) -> Vec<u8> {
    let pad = BLOCK - data.len() % BLOCK;
    let mut out = Vec::with_capacity(data.len() + pad);
    out.extend_from_slice(data);
    out.resize(data.len() + pad, pad as u8);
    out
}

fn pkcs7_unpad(data: &[u8]) -> Result<&[u8]> {
    let Some(&last) = data.last() else {
        bail!("empty ciphertext")
    };
    let pad = last as usize;
    if pad == 0 || pad > BLOCK || pad > data.len() {
        bail!("bad PKCS#7 padding");
    }
    if data[data.len() - pad..].iter().any(|&b| b as usize != pad) {
        bail!("bad PKCS#7 padding");
    }
    Ok(&data[..data.len() - pad])
}

fn cbc_encrypt(cipher: &Cipher, iv: &[u8], plaintext: &[u8]) -> Vec<u8> {
    let mut buf = pkcs7_pad(plaintext);
    let mut prev = [0u8; BLOCK];
    prev.copy_from_slice(iv);
    for block in buf.as_chunks_mut::<BLOCK>().0 {
        for (b, p) in block.iter_mut().zip(prev.iter()) {
            *b ^= p;
        }
        cipher.encrypt_block(block);
        prev.copy_from_slice(block);
    }
    buf
}

fn ecb_encrypt(cipher: &Cipher, plaintext: &[u8]) -> Vec<u8> {
    let mut buf = pkcs7_pad(plaintext);
    for block in buf.as_chunks_mut::<BLOCK>().0 {
        cipher.encrypt_block(block);
    }
    buf
}

pub fn matdesc(material: &EncryptionMaterial, key_size_bits: usize) -> String {
    // Same key order and string-typed numbers as the drivers.
    format!(
        "{{\"queryId\":{},\"smkId\":\"{}\",\"keySize\":\"{}\"}}",
        serde_json::Value::String(material.query_id.clone()),
        material.smk_id,
        key_size_bits
    )
}

/// Encrypts `plaintext` (the bytes that end up on the stage, e.g. gzip data).
pub fn encrypt(
    material: &EncryptionMaterial,
    plaintext: &[u8],
) -> Result<(Vec<u8>, EncryptionMetadata)> {
    let master = B64.decode(&material.query_stage_master_key)?;
    let master_cipher = Cipher::new(&master)?;
    let file_key = random_bytes(master.len());
    let iv = random_bytes(BLOCK);

    let ciphertext = cbc_encrypt(&Cipher::new(&file_key)?, &iv, plaintext);
    let enc_key = ecb_encrypt(&master_cipher, &file_key);

    Ok((
        ciphertext,
        EncryptionMetadata {
            key: B64.encode(enc_key),
            iv: B64.encode(iv),
            matdesc: matdesc(material, master.len() * 8),
        },
    ))
}

/// Inverse of [`encrypt`]; used by tests and the fixture self-check.
pub fn decrypt(
    material: &EncryptionMaterial,
    meta: &EncryptionMetadata,
    ciphertext: &[u8],
) -> Result<Vec<u8>> {
    let master = B64.decode(&material.query_stage_master_key)?;
    let master_cipher = Cipher::new(&master)?;
    let mut key_buf = B64.decode(&meta.key)?;
    if key_buf.is_empty() || !key_buf.len().is_multiple_of(BLOCK) {
        bail!("bad encrypted key length");
    }
    for block in key_buf.as_chunks_mut::<BLOCK>().0 {
        master_cipher.decrypt_block(block);
    }
    let file_key = pkcs7_unpad(&key_buf)?.to_vec();
    let iv = B64.decode(&meta.iv)?;
    if iv.len() != BLOCK || ciphertext.is_empty() || !ciphertext.len().is_multiple_of(BLOCK) {
        bail!("bad IV or ciphertext length");
    }
    let file_cipher = Cipher::new(&file_key)?;
    let mut buf = ciphertext.to_vec();
    let mut prev = [0u8; BLOCK];
    prev.copy_from_slice(&iv);
    for block in buf.as_chunks_mut::<BLOCK>().0 {
        let mut this = [0u8; BLOCK];
        this.copy_from_slice(block);
        file_cipher.decrypt_block(block);
        for (b, p) in block.iter_mut().zip(prev.iter()) {
            *b ^= p;
        }
        prev = this;
    }
    Ok(pkcs7_unpad(&buf)?.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn material(key_len: usize) -> EncryptionMaterial {
        EncryptionMaterial {
            query_stage_master_key: B64.encode(random_bytes(key_len)),
            query_id: "01b2c3d4-0000-0000-0000-000000000000".into(),
            smk_id: 12345,
        }
    }

    #[test]
    fn roundtrip_all_lengths() {
        for key_len in [16, 32] {
            let m = material(key_len);
            for len in [0usize, 1, 15, 16, 17, 64 * 1024, 100_003] {
                let plain: Vec<u8> = (0..len).map(|i| (i * 7 % 251) as u8).collect();
                let (ct, meta) = encrypt(&m, &plain).unwrap();
                assert_eq!(ct.len(), (len / 16 + 1) * 16, "always padded");
                assert_eq!(B64.decode(&meta.key).unwrap().len(), key_len + 16);
                assert_eq!(decrypt(&m, &meta, &ct).unwrap(), plain);
            }
        }
    }

    #[test]
    fn matdesc_matches_driver_format() {
        let m = material(32);
        let (_, meta) = encrypt(&m, b"x").unwrap();
        assert_eq!(
            meta.matdesc,
            r#"{"queryId":"01b2c3d4-0000-0000-0000-000000000000","smkId":"12345","keySize":"256"}"#
        );
    }

    #[test]
    fn cbc_matches_nist_vector() {
        // NIST SP 800-38A F.2.5 (CBC-AES256.Encrypt), first block.
        let key = hex::decode("603deb1015ca71be2b73aef0857d77811f352c073b6108d72d9810a30914dff4")
            .unwrap();
        let iv = hex::decode("000102030405060708090a0b0c0d0e0f").unwrap();
        let pt = hex::decode("6bc1bee22e409f96e93d7e117393172a").unwrap();
        let ct = cbc_encrypt(&Cipher::new(&key).unwrap(), &iv, &pt);
        assert_eq!(hex::encode(&ct[..16]), "f58c4c04d6e5f1ba779eabfb5f7bfbd6");
    }

    #[test]
    fn wrong_master_key_fails() {
        let m = material(32);
        let (ct, meta) = encrypt(&m, b"hello world").unwrap();
        let other = material(32);
        assert!(
            decrypt(&other, &meta, &ct)
                .map(|p| p != b"hello world")
                .unwrap_or(true)
        );
    }
}
