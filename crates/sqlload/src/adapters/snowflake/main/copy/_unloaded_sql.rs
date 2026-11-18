//! The statements loading a Snowflake source's rows into a table inside Snowflake: from its unloaded Parquet files, or selected from the source in the same session.

use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::snowflake::_helpers::dialect::unloaded_values::column_value;
use crate::adapters::snowflake::constants::{COPY_FILE_FORMAT, MAX_COPY_FILES};
use crate::adapters::snowflake::models::{SnowflakeSql, UnloadedFrom, UnloadedLoad, UnloadedRows};

/// The statements loading `request`'s rows into `request.into`, every one of its columns filled as [`column_value`] says: from files, `COPY`s of at most 1,000 files each, stopping at the first bad value and never relying on Snowflake's load history; from a query, one `INSERT ... SELECT`.
pub(in crate::adapters) fn unloaded_statements(request: &UnloadedLoad<'_>) -> Vec<String> {
    let into = SnowflakeSql.render_qualified_name(request.into);
    let names: Vec<String> = request
        .columns
        .iter()
        .map(|column| SnowflakeSql.render_identifier(&column.name))
        .collect();
    let names = names.join(", ");
    match &request.from {
        UnloadedFrom::Files {
            stage,
            folder,
            keys,
            ..
        } => keys
            .chunks(MAX_COPY_FILES)
            .enumerate()
            .map(|(chunk, chunk_keys)| {
                format!(
                    "COPY INTO {into} ({names}) FROM (SELECT {} FROM {stage}/{folder}) FILES = ({}) {COPY_FILE_FORMAT} ON_ERROR = ABORT_STATEMENT FORCE = TRUE",
                    values(request, UnloadedRows::Files(chunk * MAX_COPY_FILES, chunk_keys)),
                    file_list(chunk_keys)
                )
            })
            .collect(),
        UnloadedFrom::Query(query) => vec![format!(
            "INSERT INTO {into} ({names}) SELECT {} FROM ({query}) AS u",
            values(request, UnloadedRows::Query)
        )],
    }
}

/// Each of `request`'s columns as a statement reading `rows` fills it.
fn values(request: &UnloadedLoad<'_>, rows: UnloadedRows<'_>) -> String {
    let values: Vec<String> = request
        .columns
        .iter()
        .map(|column| column_value(request, column, &rows))
        .collect();
    values.join(", ")
}

/// `keys` as the `FILES` list of a `COPY`.
fn file_list(keys: &[String]) -> String {
    let files: Vec<String> = keys
        .iter()
        .map(|key| SnowflakeSql.render_loader_value_literal(key))
        .collect();
    files.join(", ")
}
