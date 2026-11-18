//! Connections to a DuckDB source's database: a file read-only, or a MotherDuck database on the MotherDuck bridge.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock, PoisonError};

use crate::adapters::dispatch::main::session::open_source_bridge::open_source_bridge;
use crate::adapters::motherduck_bridge::main::fork_connection::fork_connection;
use crate::adapters::motherduck_bridge::main::run_statement::run_statement;
use crate::adapters::motherduck_bridge::models::BridgeConnection;
use crate::duckdb_source::errors::DuckDbSourceError;
use crate::duckdb_source::models::{DuckDbReader, FileHold, OpenFile, ReadOnlyFile, SourceBridge};
use crate::memory_budget::main::watch_child::watch_child;

/// Rows of text values, `None` for `NULL`.
pub(crate) type TextRows = Vec<Vec<Option<String>>>;

/// Each database file a source reads now, by its canonical path, with the one instance this process holds on it. DuckDB locks a file with POSIX locks, which belong to the process: closing a second instance on the file would drop the first one's lock too, letting another process write the file mid-read.
static OPEN_FILES: OnceLock<Mutex<HashMap<PathBuf, OpenFile>>> = OnceLock::new();

/// A connection to the reader's database file, opened read-only, so the file is never changed and SQLLoad holds no write lock on it. Reads open at the same time share one instance, given the reader's `memory_limit` and a temporary folder of its own to spill into (not the default folder beside the file).
pub(crate) fn open_read_only(reader: &DuckDbReader) -> Result<ReadOnlyFile, DuckDbSourceError> {
    let path = std::fs::canonicalize(&reader.database)
        .map_err(|error| DuckDbSourceError::Invalid(format!("{}: {error}", reader.database)))?;
    let files = OPEN_FILES.get_or_init(Mutex::default);
    let mut open = files.lock().unwrap_or_else(PoisonError::into_inner);
    let file = match open.entry(path.clone()) {
        Entry::Occupied(entry) => entry.into_mut(),
        Entry::Vacant(entry) => {
            let spill = tempfile::Builder::new()
                .prefix("sqlload-duckdb-source-")
                .tempdir()
                .map_err(|error| DuckDbSourceError::Invalid(error.to_string()))?;
            let config = duckdb::Config::default()
                .access_mode(duckdb::AccessMode::ReadOnly)?
                .with("temp_directory", spill.path().display().to_string())?;
            let connection = duckdb::Connection::open_with_flags(&path, config)?;
            if let Some(limit) = reader.memory_limit {
                connection.execute_batch(&format!("SET memory_limit = '{}KiB'", limit / 1024))?;
            }
            entry.insert(OpenFile {
                reads: 0,
                connection,
                _spill: spill,
            })
        }
    };
    let connection = file.connection.try_clone()?;
    file.reads += 1;
    Ok(ReadOnlyFile {
        connection,
        _hold: FileHold { path },
    })
}

/// The last read of a file to end closes its instance, while it holds the open files' lock, so no new instance on the file opens before this one has closed.
impl Drop for FileHold {
    fn drop(&mut self) {
        let files = OPEN_FILES.get_or_init(Mutex::default);
        let mut open = files.lock().unwrap_or_else(PoisonError::into_inner);
        if let Entry::Occupied(mut entry) = open.entry(self.path.clone()) {
            entry.get_mut().reads -= 1;
            if entry.get().reads == 0 {
                drop(entry.remove());
            }
        }
    }
}

/// A connection of its own to a `motherduck` reader's database, on the reader's bridge: opened on its first use, signed in as a MotherDuck target is, its DuckDB given the reader's `memory_limit`, and its memory counted by the load's watchdog while it is open.
pub(crate) async fn open_bridge(
    reader: &DuckDbReader,
) -> Result<BridgeConnection, DuckDbSourceError> {
    let Some(connection) = reader.motherduck.clone() else {
        return Err(DuckDbSourceError::Invalid(format!(
            "{} is a file, not a MotherDuck database",
            reader.database
        )));
    };
    let (memory_limit, guard) = (reader.memory_limit, reader.guard.clone());
    let shared = reader
        .bridge
        .get_or_try_init(|| async move {
            let bridge = tokio::task::spawn_blocking(move || open_source_bridge(&connection))
                .await
                .map_err(|error| {
                    DuckDbSourceError::Invalid(format!("opening MotherDuck stopped: {error}"))
                })?
                .map_err(|error| DuckDbSourceError::Invalid(error.to_string()))?;
            if let Some(limit) = memory_limit {
                let set = format!("SET memory_limit = '{}KiB'", limit / 1024);
                run_statement(&bridge, set, Vec::new())
                    .await
                    .map_err(DuckDbSourceError::Bridge)?;
            }
            let watched = guard.map(|guard| watch_child(&guard, bridge.cursor.process.pid));
            Ok::<_, DuckDbSourceError>(SourceBridge {
                connection: bridge,
                _watched: watched,
            })
        })
        .await?;
    Ok(fork_connection(&shared.connection))
}

/// Each of `queries`' rows, run in order on one connection to the reader's database, every value as text; a failure names the index of the query that failed. Each query casts its columns to `VARCHAR`.
pub(crate) async fn text_rows(
    reader: &DuckDbReader,
    queries: Vec<String>,
) -> Result<Vec<TextRows>, (usize, DuckDbSourceError)> {
    if reader.motherduck.is_none() {
        let reader = reader.clone();
        return tokio::task::spawn_blocking(move || {
            let connection = open_read_only(&reader).map_err(|error| (0, error))?;
            queries
                .iter()
                .enumerate()
                .map(|(index, sql)| {
                    local_rows(&connection.connection, sql).map_err(|error| (index, error.into()))
                })
                .collect()
        })
        .await
        .map_err(|error| {
            (
                0,
                DuckDbSourceError::Invalid(format!("the query stopped: {error}")),
            )
        })?;
    };
    let bridge = open_bridge(reader).await.map_err(|error| (0, error))?;
    let mut answers = Vec::with_capacity(queries.len());
    for (index, sql) in queries.into_iter().enumerate() {
        let rows = run_statement(&bridge, sql, Vec::new())
            .await
            .map_err(|error| (index, DuckDbSourceError::Bridge(error)))?;
        answers.push(rows);
    }
    Ok(answers)
}

/// `sql`'s rows on a file's connection, each value as text.
fn local_rows(connection: &duckdb::Connection, sql: &str) -> Result<TextRows, duckdb::Error> {
    let mut statement = connection.prepare(sql)?;
    let mut rows = statement.query([])?;
    let mut answer: TextRows = Vec::new();
    while let Some(row) = rows.next()? {
        let width = row.as_ref().column_count();
        answer.push(
            (0..width)
                .map(|index| row.get::<_, Option<String>>(index))
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    Ok(answer)
}
