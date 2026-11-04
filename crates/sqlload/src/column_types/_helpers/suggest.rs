use crate::column_types::_helpers::names::known_names;
use crate::column_types::constants::PORTABLE_NAMES;
use crate::similar_names::main::nearest_name::nearest_name;

/// The portable type name closest to `name`, as written in [`PORTABLE_NAMES`].
pub(crate) fn nearest_portable_name(name: &str) -> String {
    let nearest = nearest_name(name, known_names()).unwrap_or("text");
    PORTABLE_NAMES
        .iter()
        .find(|portable| portable.split('(').next() == Some(nearest))
        .map_or_else(|| nearest.to_owned(), |portable| (*portable).to_owned())
}
