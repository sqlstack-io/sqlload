//! What a DuckDB or MotherDuck source reads, for `debug`.

use crate::duckdb_source::_helpers::connection::text_rows;
use crate::duckdb_source::errors::DuckDbSourceError;
use crate::duckdb_source::models::DuckDbReader;

/// The reader's database and the DuckDB version reading it, once the database has opened: a file read-only, a MotherDuck database on the bridge.
pub async fn describe_source(reader: &DuckDbReader) -> Result<String, DuckDbSourceError> {
    let answers = text_rows(reader, vec!["SELECT version()".to_owned()])
        .await
        .map_err(|(_, error)| error)?;
    let version = answers
        .first()
        .and_then(|rows| rows.first())
        .and_then(|row| row.first().cloned().flatten())
        .unwrap_or_default();
    Ok(match reader.motherduck {
        Some(_) => format!(
            "{} on MotherDuck, through DuckDB {version}",
            reader.database
        ),
        None => format!("{}, read-only, DuckDB {version}", reader.database),
    })
}
