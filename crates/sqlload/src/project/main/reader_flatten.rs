//! How the files reader flattens a table's JSON records.

use crate::load_naming::models::{Compat, Naming};
use crate::project::main::table_flatten::table_flatten;
use crate::project::models::SourceConfig;
use crate::row_batches::models::Flatten;

/// How the files reader flattens the JSON records of `source`'s table `name`: as its `flatten` says (see [`table_flatten`]), or not at all when a `transform` runs, whose output flattens instead.
pub fn reader_flatten(
    source: &SourceConfig,
    name: &str,
    (naming, compat): (Naming, Compat),
) -> Flatten {
    let overrides = source.tables.get(name);
    match overrides
        .and_then(|table| table.settings.transform.as_ref())
        .or(source.defaults.transform.as_ref())
    {
        Some(_) => Flatten::default(),
        None => table_flatten((source, overrides), (compat, naming, false)).0,
    }
}
