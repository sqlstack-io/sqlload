use crate::cdc::models::TupleValue;

/// A NUL-terminated string.
pub(crate) fn cstring(text: &str) -> Vec<u8> {
    [text.as_bytes(), &[0]].concat()
}

/// The encoded part of a NULL value.
pub(crate) const NULL_PART: &[u8] = b"n";
/// The encoded part of an unchanged TOAST value.
pub(crate) const UNCHANGED_PART: &[u8] = b"u";

/// The encoded part of a text value.
pub(crate) fn text_part(value: &str) -> Vec<u8> {
    let length = i32::try_from(value.len()).unwrap_or(i32::MAX);
    [b"t".as_slice(), &length.to_be_bytes(), value.as_bytes()].concat()
}

/// pgoutput TupleData of the encoded `parts`.
pub(crate) fn tuple(parts: &[&[u8]]) -> Vec<u8> {
    let count = i16::try_from(parts.len()).unwrap_or(i16::MAX);
    [count.to_be_bytes().to_vec(), parts.concat()].concat()
}

/// A text value.
pub(crate) fn text(value: &str) -> TupleValue {
    TupleValue::Text(value.to_owned())
}
