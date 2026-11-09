//! Open a DuckDB database.

use crate::adapters::duckdb::_helpers::shared_destination::shared_destination;
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::DuckDbDestination;
use crate::project::main::motherduck_path::motherduck_path;

/// Open (or create) the database at `path`; `:memory:` for an in-memory database. A MotherDuck path is refused: a `motherduck` connection opens it, with its token and routes.
pub(in crate::adapters) fn open_duckdb(path: &str) -> Result<DuckDbDestination, DuckDbError> {
    if motherduck_path(path).prefixed {
        return Err(DuckDbError::MotherDuck(
            "a duckdb connection's database is a file or :memory:; for a MotherDuck database use kind = \"motherduck\"".to_owned(),
        ));
    }
    let connection = match path {
        ":memory:" => duckdb::Connection::open_in_memory()?,
        path => duckdb::Connection::open(path)?,
    };
    shared_destination(connection, false)
}
