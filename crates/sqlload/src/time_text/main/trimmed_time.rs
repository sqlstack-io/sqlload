//! Timestamps as `plan` shows them.

use crate::time_text::constants::{HOURS_OFFSET_LENGTH, SECOND_TIMESTAMP_LENGTH};

/// `value` with a timestamp's fraction of a second cut to the second, or to the millisecond when `precise`, and an offset of whole hours shortened (`+00:00` to `+00`); anything else as it is.
pub fn trimmed_time(value: &str, precise: bool) -> String {
    let bytes = value.as_bytes();
    let is_timestamp = bytes.len() >= SECOND_TIMESTAMP_LENGTH
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && matches!(bytes[10], b' ' | b'T')
        && bytes[13] == b':'
        && bytes[16] == b':'
        && value.is_char_boundary(SECOND_TIMESTAMP_LENGTH);
    if !is_timestamp {
        return value.to_owned();
    }
    let (seconds, rest) = value.split_at(SECOND_TIMESTAMP_LENGTH);
    let digits = match rest.strip_prefix('.') {
        Some(fraction) => fraction.bytes().take_while(u8::is_ascii_digit).count(),
        None => 0,
    };
    let (fraction, offset) = match digits {
        0 => ("", rest),
        _ => rest.split_at(digits + 1),
    };
    let fraction = match precise {
        true => fraction.get(..4).unwrap_or(fraction),
        false => "",
    };
    let offset = offset
        .strip_suffix(":00")
        .filter(|hours| hours.len() == HOURS_OFFSET_LENGTH)
        .unwrap_or(offset);
    format!("{seconds}{fraction}{offset}")
}
