//! Connections to a DuckDB source's database: a file read-only, or a MotherDuck database on the MotherDuck bridge.

use crate::adapters::dispatch::main::session::open_source_bridge::open_source_bridge;
use crate::adapters::motherduck_bridge::main::fork_connection::fork_connection;
use crate::adapters::motherduck_bridge::main::run_statement::run_statement;
use crate::adapters::motherduck_bridge::models::BridgeConnection;
use crate::duckdb_source::errors::DuckDbSourceError;
use crate::duckdb_source::models::DuckDbReader;

/// Rows of text values, `None` for `NULL`.
pub(crate) type TextRows = Vec<Vec<Option<String>>>;

/// The reader's database opened read-only, so a file another process writes is never changed and SQLLoad holds no write lock on it, with the reader's `memory_limit`.
pub(crate) fn open_read_only(
    reader: &DuckDbReader,
) -> Result<duckdb::Connection, DuckDbSourceError> {
    let config = duckdb::Config::default().access_mode(duckdb::AccessMode::ReadOnly)?;
    let connection = duckdb::Connection::open_with_flags(&reader.database, config)?;
    if let Some(limit) = reader.memory_limit {
        connection.execute_batch(&format!("SET memory_limit = '{}KiB'", limit / 1024))?;
    }
    Ok(connection)
}

/// A connection of its own to a `motherduck` reader's database, on the reader's bridge: opened on its first use, signed in as a MotherDuck target is, its DuckDB given the reader's `memory_limit`.
pub(crate) async fn open_bridge(
    reader: &DuckDbReader,
) -> Result<BridgeConnection, DuckDbSourceError> {
    let Some(connection) = reader.motherduck.clone() else {
        return Err(DuckDbSourceError::Invalid(format!(
            "{} is a file, not a MotherDuck database",
            reader.database
        )));
    };
    let memory_limit = reader.memory_limit;
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
            Ok::<_, DuckDbSourceError>(bridge)
        })
        .await?;
    Ok(fork_connection(shared))
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
                    local_rows(&connection, sql).map_err(|error| (index, error.into()))
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
