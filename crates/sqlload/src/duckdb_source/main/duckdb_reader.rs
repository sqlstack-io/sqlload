//! A reader for a `duckdb` or `motherduck` source.

use crate::duckdb_source::constants::MEMORY_DATABASE;
use crate::duckdb_source::errors::DuckDbSourceError;
use crate::duckdb_source::models::DuckDbReader;
use crate::project::main::motherduck_path::motherduck_path;
use crate::project::models::{ConnectionConfig, ProjectConfig, SourceConfig};

/// A reader for `source`'s connection: a `duckdb` connection's database file, which each read opens read-only with a `memory_limit` of half a table's share of the project's memory budget (DuckDB's own default without one), or a `motherduck` connection's database, which each read opens on the MotherDuck bridge. Refused: `:memory:`, which holds no tables to read; a MotherDuck path on a `duckdb` connection, which is a `motherduck` source; and a `motherduck` connection naming no database, as a source reads one. Nothing is opened yet.
pub fn duckdb_reader(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<DuckDbReader, DuckDbSourceError> {
    let memory_limit = project.memory.budget.map(|budget| budget.table_share / 2);
    let database = match project.connections.get(&source.connection) {
        Some(ConnectionConfig::DuckDb { database }) => database,
        Some(connection @ ConnectionConfig::MotherDuck { database, .. }) => {
            let database = database
                .as_deref()
                .filter(|database| motherduck_path(database).name.is_some())
                .ok_or_else(|| {
                    DuckDbSourceError::Invalid(format!(
                        "a motherduck source reads one database: name it in connection {:?}'s database",
                        source.connection
                    ))
                })?;
            return Ok(DuckDbReader {
                database: database.to_owned(),
                memory_limit,
                motherduck: Some(connection.clone()),
                bridge: std::sync::Arc::default(),
                guard: project.memory.guard.clone(),
            });
        }
        _ => {
            return Err(DuckDbSourceError::Invalid(format!(
                "connection {:?} is not a duckdb or motherduck connection",
                source.connection
            )));
        }
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
        memory_limit,
        motherduck: None,
        bridge: std::sync::Arc::default(),
        guard: project.memory.guard.clone(),
    })
}
