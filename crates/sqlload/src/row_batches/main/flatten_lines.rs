//! What `plan` says a table's `flatten` does.

use crate::load_naming::main::loaded_name::loaded_name;
use crate::row_batches::_helpers::flatten::{keys_are_data, leaf_keys};
use crate::row_batches::constants::{FLATTEN_SEPARATOR, SHOWN_OBJECT_KEYS};
use crate::row_batches::models::{Flatten, FlattenObjects};

/// The lines of a table's `flatten` detail: what it flattens and how deep; then, given the `objects` flattening replaced in the sampled `columns` (names as the data spells them), one line per object with its column count and first keys as they load, the columns kept whole as JSON (`kept`, loaded names), and under `"all"` a warning when an object looks like a map whose keys are data (see [`keys_are_data`]); without a sample, that objects flatten at load. No lines when nothing flattens.
pub fn flatten_lines(
    flatten: &Flatten,
    sample: Option<(&[String], &[String])>,
    kept: &[String],
) -> Vec<String> {
    let setting = match &flatten.objects {
        FlattenObjects::None => return Vec::new(),
        FlattenObjects::All => format!("every object, {} levels down at most", flatten.max_depth),
        FlattenObjects::Paths(paths) => paths.join(", "),
    };
    let Some((objects, columns)) = sample else {
        return vec![format!("{setting}; objects flatten as rows arrive at load")];
    };
    let mut lines: Vec<String> = vec![setting];
    let shown: Vec<(String, Vec<String>)> = objects
        .iter()
        .map(|object| {
            (
                object.replace(FLATTEN_SEPARATOR, "."),
                leaf_keys(object, (objects, columns)),
            )
        })
        .collect();
    let width = shown
        .iter()
        .map(|(path, _)| path.chars().count())
        .max()
        .unwrap_or_default();
    for (path, keys) in &shown {
        let first: Vec<String> = keys
            .iter()
            .take(SHOWN_OBJECT_KEYS)
            .map(|key| loaded_name(flatten.naming, key, None))
            .collect();
        let more = match keys.len() > SHOWN_OBJECT_KEYS {
            true => ", …",
            false => "",
        };
        let count = match keys.len() {
            1 => "1 column".to_owned(),
            count => format!("{count} columns"),
        };
        lines.push(format!(
            "{path:<width$}  → {count} ({}{more})",
            first.join(", ")
        ));
    }
    if !kept.is_empty() {
        lines.push(format!("kept as JSON: {}", kept.join(", ")));
    }
    let widest = shown
        .iter()
        .filter(|(_, keys)| keys_are_data(keys))
        .max_by_key(|(_, keys)| keys.len());
    if let (FlattenObjects::All, Some((path, _))) = (&flatten.objects, widest) {
        let name = path.replace('.', FLATTEN_SEPARATOR);
        let name = loaded_name(flatten.naming, &name, None);
        lines.push(
            "⚠ an object whose keys are data adds a column per key on every load;".to_owned(),
        );
        lines.push(format!(
            "  keep it as one column with columns = {{ {name} = \"json\" }}"
        ));
    }
    lines
}
