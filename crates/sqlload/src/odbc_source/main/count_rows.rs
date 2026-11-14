//! Count a table's rows that a condition selects.

use arrow_odbc::odbc_api::Cursor;

use crate::odbc_source::_helpers::connect::connect;
use crate::odbc_source::_helpers::query::count_query;
use crate::odbc_source::errors::OdbcSourceError;
use crate::odbc_source::models::OdbcReader;
use crate::postgres_source::models::SourceTable;

/// How many rows of `table` (or of its query `sql`) `filter` selects, counted by the database.
pub async fn count_rows(
    reader: &OdbcReader,
    (table, sql): (&SourceTable, Option<&str>),
    filter: &str,
) -> Result<u64, OdbcSourceError> {
    let (reader, query) = (reader.clone(), count_query(table, sql, filter));
    tokio::task::spawn_blocking(move || {
        let connection = connect(&reader)?;
        let Some(mut cursor) = connection.execute(&query, (), None)? else {
            return Err(OdbcSourceError::Invalid(format!(
                "{query} returned no rows"
            )));
        };
        let mut buffer: Vec<u8> = Vec::new();
        let Some(mut row) = cursor.next_row()? else {
            return Err(OdbcSourceError::Invalid(format!(
                "{query} returned no count"
            )));
        };
        row.get_text(1, &mut buffer)?;
        let count = String::from_utf8_lossy(&buffer).trim().to_owned();
        count.parse::<u64>().map_err(|error| {
            OdbcSourceError::Invalid(format!("{query} returned the count {count:?}: {error}"))
        })
    })
    .await
    .map_err(|error| OdbcSourceError::Invalid(format!("the ODBC count stopped: {error}")))?
}
