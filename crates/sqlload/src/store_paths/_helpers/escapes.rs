/// Undo the `%XX` escapes Hive and Spark write into partition values; anything else is kept as written.
pub(crate) fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match escaped_byte(bytes, index) {
            Some(byte) => {
                decoded.push(byte);
                index += 3;
            }
            None => {
                decoded.push(bytes[index]);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

/// The byte a `%XX` escape at `index` stands for, if there is one.
fn escaped_byte(bytes: &[u8], index: usize) -> Option<u8> {
    if bytes[index] != b'%' {
        return None;
    }
    let high = hex_digit(*bytes.get(index + 1)?)?;
    let low = hex_digit(*bytes.get(index + 2)?)?;
    Some(high * 16 + low)
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
