//! The closest candidate to a misspelt name.

use crate::similar_names::_helpers::distance::edit_distance;

/// The candidate with the smallest edit distance to `name`, if there are any candidates.
pub fn nearest_name<'a>(
    name: &str,
    candidates: impl IntoIterator<Item = &'a str>,
) -> Option<&'a str> {
    candidates
        .into_iter()
        .min_by_key(|candidate| edit_distance(name, candidate))
}
