//! Open a DuckDB database.

use crate::adapters::duckdb::_helpers::shared_destination::shared_destination;
use crate::adapters::duckdb::constants::{CHECKPOINT_STATEMENT, NO_AUTOCHECKPOINT, WAL_SUFFIX};
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::DuckDbDestination;
use crate::memory_budget::models::DuckDbShare;
use crate::number_text::main::memory_text::memory_text;
use crate::project::main::motherduck_path::motherduck_path;

/// Open (or create) the database at `path`; `:memory:` for an in-memory database. A MotherDuck path is refused: a `motherduck` connection opens it, with its token and routes. Under `memory`, its part of the run's memory budget, DuckDB gets that `memory_limit` and thread count, checkpoints only when SQLLoad says, and first checkpoints what an earlier run left in its write-ahead log; when that doesn't fit, the target fails, naming the `memory_limit` that would.
pub(in crate::adapters) fn open_duckdb(
    path: &str,
    memory: Option<DuckDbShare>,
) -> Result<DuckDbDestination, DuckDbError> {
    if motherduck_path(path).prefixed {
        return Err(DuckDbError::MotherDuck(
            "a duckdb connection's database is a file or :memory:; for a MotherDuck database use kind = \"motherduck\"".to_owned(),
        ));
    }
    let connection = match path {
        ":memory:" => duckdb::Connection::open_in_memory()?,
        path => duckdb::Connection::open(path)?,
    };
    if let Some(share) = memory {
        connection.execute_batch(&format!(
            "{NO_AUTOCHECKPOINT}; SET memory_limit = '{}KiB'; SET threads = {}",
            share.memory_limit / 1024,
            share.threads
        ))?;
        if let Err(error) = connection.execute_batch(CHECKPOINT_STATEMENT) {
            let wal = std::fs::metadata(format!("{path}{WAL_SUFFIX}")).map_or(0, |file| file.len());
            return Err(DuckDbError::Task(format!(
                "{path}: DuckDB could not write the {} an earlier run left in its write-ahead log ({path}{WAL_SUFFIX}) within its {} memory_limit ({error}); raise memory_limit for one run until plan's memory line gives DuckDB a memory_limit of about {}, which writes it",
                memory_text(wal),
                memory_text(share.memory_limit),
                memory_text(
                    wal.saturating_mul(2)
                        .max(share.memory_limit.saturating_mul(2))
                )
            )));
        }
    }
    let own_limit = match memory {
        Some(_) => None,
        None => Some(own_memory_limit(&connection)?),
    };
    let mut destination = shared_destination(connection)?;
    destination.memory = memory;
    destination.own_limit = own_limit;
    Ok(destination)
}

/// The `memory_limit` DuckDB set itself (80% of the machine's memory by default); in a test build, `SQLLOAD_TEST_DUCKDB_MEMORY_LIMIT` sets it first, so a test reaches it.
fn own_memory_limit(connection: &duckdb::Connection) -> Result<u64, DuckDbError> {
    #[cfg(feature = "container-tests")]
    if let Ok(limit) = std::env::var("SQLLOAD_TEST_DUCKDB_MEMORY_LIMIT") {
        connection.execute_batch(&format!("SET memory_limit = '{limit}'"))?;
    }
    let text: String =
        connection.query_row("SELECT current_setting('memory_limit')", [], |row| {
            row.get(0)
        })?;
    crate::config::_helpers::units::parse_byte_size(&text)
        .map_err(|error| DuckDbError::Task(format!("DuckDB's memory_limit {text:?}: {error}")))
}
