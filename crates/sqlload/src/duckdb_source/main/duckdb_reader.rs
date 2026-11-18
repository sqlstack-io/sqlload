//! A reader for a `duckdb` source's connection.

use crate::duckdb_source::errors::DuckDbSourceError;
use crate::duckdb_source::models::DuckDbReader;
use crate::project::main::motherduck_path::motherduck_path;
use crate::project::models::ConnectionConfig;

/// A reader for a `duckdb` connection's database file; each read opens it read-only, with `memory_limit` (half a table's share of the memory budget, or DuckDB's own default without one). `:memory:` and MotherDuck paths are refused: an empty in-memory database has nothing to read, and a MotherDuck database is a `motherduck` source. Nothing is opened yet.
pub fn duckdb_reader(
    connection: &ConnectionConfig,
    memory_limit: Option<u64>,
) -> Result<DuckDbReader, DuckDbSourceError> {
    let ConnectionConfig::DuckDb { database } = connection else {
        return Err(DuckDbSourceError::Invalid(format!(
            "a {} connection is not a duckdb one",
            connection.kind()
        )));
    };
    if database == ":memory:" {
        return Err(DuckDbSourceError::Invalid(
            "a duckdb source reads a database file; :memory: holds no tables to read".to_owned(),
        ));
    }
    if motherduck_path(database).prefixed {
        return Err(DuckDbSourceError::Invalid(
            "a duckdb connection's database is a file; for a MotherDuck database use kind = \"motherduck\"".to_owned(),
        ));
    }
    Ok(DuckDbReader {
        database: database.clone(),
        memory_limit,
    })
}
