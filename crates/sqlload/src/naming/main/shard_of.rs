//! Assign streams to instances.

use sha2::{Digest, Sha256};

/// The shard that owns `table` among `shards`, by rendezvous hashing: changing the count moves only the streams the new shard wins.
pub fn shard_of(table: &str, shards: u32) -> u32 {
    (0..shards.max(1))
        .max_by_key(|shard| score(table, *shard))
        .unwrap_or(0)
}

fn score(table: &str, shard: u32) -> u64 {
    let digest = Sha256::digest(format!("{shard}:{table}").as_bytes());
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(bytes)
}
