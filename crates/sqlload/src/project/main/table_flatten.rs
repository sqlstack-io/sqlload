//! How a table flattens the objects of its rows, from its settings.

use crate::load_naming::models::{Compat, Naming};
use crate::project::_helpers::flatten_rules::{
    flattened_objects, kept_and_pinned, path_problem, setting_problem,
};
use crate::project::constants::DEFAULT_MAX_DEPTH;
use crate::project::models::{SourceConfig, TableOverride};
use crate::row_batches::models::{Flatten, FlattenObjects};

/// Which objects table `overrides` of `source` flattens and how deep (its `max_depth`), and the problems with its `flatten`: its own setting, else under `compat = "dlt"` every object of JSON and NDJSON files. A `json` pin, a mask, or an `exclude_columns` or `allow_columns` entry naming an object (not a pattern) keeps it whole; a pin of another type on a flattened object fails. A listed path's parent must be listed too, in `flatten` or `unnest`, and the setting needs rows that hold objects: a Python table, JSON or NDJSON files, or a table with a `transform`.
pub fn table_flatten(
    (source, overrides): (&SourceConfig, Option<&TableOverride>),
    (compat, naming, transform): (Compat, Naming, bool),
) -> (Flatten, Vec<String>) {
    let max_depth = overrides
        .and_then(|table| table.max_depth)
        .unwrap_or(DEFAULT_MAX_DEPTH);
    let (objects, mut problems) = flattened_objects((source, overrides), (compat, transform));
    let (keep, pinned) = kept_and_pinned(source, overrides);
    let flatten = Flatten {
        objects,
        keep,
        pinned,
        max_depth,
        naming,
    };
    if let (FlattenObjects::Paths(paths), Some(overrides)) = (&flatten.objects, overrides) {
        problems.extend(paths.iter().filter_map(|path| {
            path_problem(path, (paths, overrides), max_depth)
                .or_else(|| setting_problem(path, (source, overrides), &flatten))
        }));
    }
    (flatten, problems)
}
