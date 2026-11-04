//! A known name so close to another that the other is probably a misspelling of it.

use crate::similar_names::_helpers::distance::edit_distance;
use crate::similar_names::constants::MAX_CLOSE_EDITS;

/// The candidate `name` is probably a misspelling of: the same ignoring case, or at most `MAX_CLOSE_EDITS` edits away (ignoring case) when that is under half of `name`'s length.
pub fn close_name<'a>(
    name: &str,
    candidates: impl IntoIterator<Item = &'a str>,
) -> Option<&'a str> {
    let lower = name.to_lowercase();
    candidates
        .into_iter()
        .filter(|candidate| *candidate != name)
        .map(|candidate| (edit_distance(&lower, &candidate.to_lowercase()), candidate))
        .filter(|(distance, _)| {
            *distance <= MAX_CLOSE_EDITS && distance + distance < lower.chars().count()
        })
        .min_by_key(|(distance, _)| *distance)
        .map(|(_, candidate)| candidate)
}
