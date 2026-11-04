use crate::column_types::_helpers::names::known_names;
use crate::column_types::constants::PORTABLE_NAMES;

/// The portable type name closest to `name`, as written in [`PORTABLE_NAMES`].
pub(crate) fn nearest_portable_name(name: &str) -> String {
    let nearest = known_names()
        .into_iter()
        .min_by_key(|candidate| edit_distance(name, candidate))
        .unwrap_or("text");
    PORTABLE_NAMES
        .iter()
        .find(|portable| portable.split('(').next() == Some(nearest))
        .map_or_else(|| nearest.to_owned(), |portable| (*portable).to_owned())
}

/// Levenshtein distance between two short ASCII strings.
fn edit_distance(left: &str, right: &str) -> usize {
    let right: Vec<char> = right.chars().collect();
    let mut previous: Vec<usize> = (0..=right.len()).collect();
    for (row, left_char) in left.chars().enumerate() {
        let mut current = vec![row + 1; right.len() + 1];
        for (column, right_char) in right.iter().enumerate() {
            let substitution = previous[column] + usize::from(left_char != *right_char);
            current[column + 1] = substitution
                .min(previous[column + 1] + 1)
                .min(current[column] + 1);
        }
        previous = current;
    }
    previous[right.len()]
}
