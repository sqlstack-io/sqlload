//! Records gzip cannot shrink.

/// `count` JSON strings of `length` pseudo-random letters.
pub fn incompressible_values(count: usize, length: usize) -> Vec<String> {
    (0..count)
        .map(|seed| format!("\"{}\"", letters(length, seed as u64 + 1)))
        .collect()
}

fn letters(length: usize, seed: u64) -> String {
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
