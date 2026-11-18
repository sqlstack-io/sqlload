//! A reader for a `duckdb` source.

use crate::duckdb_source::constants::MEMORY_DATABASE;
use crate::duckdb_source::errors::DuckDbSourceError;
use crate::duckdb_source::models::DuckDbReader;
use crate::project::main::motherduck_path::motherduck_path;
use crate::project::models::{ConnectionConfig, ProjectConfig, SourceConfig};

/// A reader for `source`'s `duckdb` connection's database file; each read opens it read-only, with a `memory_limit` of half a table's share of the project's memory budget (DuckDB's own default without one). `:memory:` and MotherDuck paths are refused: an empty in-memory database has nothing to read, and a MotherDuck database is a `motherduck` source. Nothing is opened yet.
pub fn duckdb_reader(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<DuckDbReader, DuckDbSourceError> {
    let Some(ConnectionConfig::DuckDb { database }) = project.connections.get(&source.connection)
    else {
        return Err(DuckDbSourceError::Invalid(format!(
            "connection {:?} is not a duckdb connection",
            source.connection
        )));
    };
    if database == MEMORY_DATABASE {
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
        memory_limit: project.memory.budget.map(|budget| budget.table_share / 2),
    })
}
