//! Which item of a list a field path's index segment reads.

use crate::row_batches::_helpers::projected::list_index;

/// The position in a list of `len` items that the index segment `segment` (`[n]`, or `[-n]` from the end) names; `None` when it is not an index or is past either end.
pub fn item_position(segment: &str, len: usize) -> Option<usize> {
    let (from_end, number) = list_index(segment.strip_prefix('[')?.strip_suffix(']')?)?;
    match from_end {
        true => len.checked_sub(number).filter(|_| number > 0),
        false => (number < len).then_some(number),
    }
}
