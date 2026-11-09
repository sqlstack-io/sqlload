//! Read a MotherDuck database setting.

use crate::adapters::duckdb::constants::MOTHERDUCK_PREFIXES;

/// `path` without its `md:` or `motherduck:` prefix, in any case as DuckDB matches it; `None` when it has neither.
pub(crate) fn without_motherduck_prefix(path: &str) -> Option<&str> {
    MOTHERDUCK_PREFIXES.iter().find_map(|prefix| {
        path.get(..prefix.len())
            .filter(|start| start.eq_ignore_ascii_case(prefix))
            .map(|_| &path[prefix.len()..])
    })
}

/// The bare database name a MotherDuck `database` setting names, without its prefix and `?` options; `None` for the account's default database.
pub(crate) fn motherduck_name(database: &str) -> Option<&str> {
    let name = without_motherduck_prefix(database).unwrap_or(database);
    let name = name.split_once('?').map_or(name, |(name, _)| name);
    (!name.is_empty()).then_some(name)
}
