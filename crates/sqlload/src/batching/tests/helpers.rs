use crate::batching::models::BatchRow;

/// A row of partition 0 at `offset`.
pub(crate) fn row(offset: i64, line: String) -> BatchRow {
    BatchRow {
        topic: "shop.orders".to_owned(),
        partition: 0,
        offset,
        line,
    }
}

/// `count` rows of letters gzip cannot shrink much, each `length` long.
pub(crate) fn noisy_rows(count: usize, length: usize) -> Vec<BatchRow> {
    (0..count)
        .map(|offset| row(offset as i64, noise(length, offset as u64 + 1)))
        .collect()
}

fn noise(length: usize, seed: u64) -> String {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from(b'a' + (state % 26) as u8)
        })
        .collect()
}
