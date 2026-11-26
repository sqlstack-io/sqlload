//! Read a table's live columns through the caller's session.

use crate::adapters::contract::models::TableName;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::session::errors::SessionErrorKind;
use crate::adapters::session::main::_query_rows::query_rows;
use crate::adapters::session::models::SessionDestination;
use crate::loader::errors::LoaderError;
use crate::schema_plan::models::LiveColumn;

/// The table's columns in order, or `None` if it does not exist: the dialect's describe query, which the warehouse's own destination runs too. A query the session reports as naming something that does not exist (a BigQuery dataset before its first load) means no table yet.
pub(in crate::adapters) async fn describe_table(
    destination: &SessionDestination,
    table: &TableName,
) -> Result<Option<Vec<LiveColumn>>, LoaderError> {
    let warehouse = destination.connection.warehouse;
    let dialect = sql_adapter(warehouse);
    let Some(query) = dialect.render_describe_table(table, destination.database.as_deref()) else {
        return Err(LoaderError::Invalid(format!(
            "{} needs a database to list {}'s columns: set the target's database",
            warehouse, table.table
        )));
    };
    let rows = match query_rows(destination, query).await {
        Err(LoaderError::Session(error)) if error.kind == SessionErrorKind::NotFound => {
            return Ok(None);
        }
        rows => rows?,
    };
    let columns = dialect
        .describe_text_rows(&rows)
        .map_err(LoaderError::Invalid)?;
    Ok((!columns.is_empty()).then_some(columns))
}
