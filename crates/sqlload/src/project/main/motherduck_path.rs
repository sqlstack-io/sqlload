//! Split a MotherDuck database setting into its parts.

use crate::project::constants::MOTHERDUCK_PREFIXES;
use crate::project::models::MotherDuckPath;

/// `database` without its `md:` or `motherduck:` prefix (in any case, as DuckDB matches it), split into the bare name and the `?` options.
pub fn motherduck_path(database: &str) -> MotherDuckPath<'_> {
    let mut rest = database;
    for prefix in MOTHERDUCK_PREFIXES {
        if let Some(start) = database.get(..prefix.len())
            && start.eq_ignore_ascii_case(prefix)
        {
            rest = &database[prefix.len()..];
        }
    }
    let (name, options) = rest.split_once('?').unwrap_or((rest, ""));
    MotherDuckPath {
        prefixed: rest.len() != database.len(),
        name: (!name.is_empty()).then_some(name),
        options,
    }
}
