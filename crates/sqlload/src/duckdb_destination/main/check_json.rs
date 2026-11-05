//! Check a batch's JSON columns before they are appended.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema};
use duckdb::vtab::ArrowBatchRegistration;

use crate::duckdb_destination::_helpers::blocking::with_connection;
use crate::duckdb_destination::errors::DuckDbError;
use crate::duckdb_destination::models::DuckDbDestination;

/// Fail when a value of the `columns` text in `batch` is not JSON by DuckDB's own rule, since its appender stores `JSON` as plain text without checking.
pub async fn check_json(
    destination: &DuckDbDestination,
    batch: &RecordBatch,
    columns: Vec<String>,
) -> Result<(), DuckDbError> {
    let fields = batch.schema();
    let texts: Vec<(String, usize)> = columns
        .into_iter()
        .filter_map(|column| {
            let (index, field) = fields.column_with_name(&column)?;
            matches!(
                field.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            )
            .then_some((column, index))
        })
        .collect();
    if texts.is_empty() {
        return Ok(());
    }
    let batch = batch.clone();
    with_connection(destination, move |connection| {
        for (column, index) in texts {
            let field = batch.schema().field(index).clone().with_name("value");
            let single = RecordBatch::try_new(
                Arc::new(Schema::new(vec![field])),
                vec![batch.column(index).clone()],
            )
            .map_err(|error| DuckDbError::Task(error.to_string()))?;
            let invalid: u64 = connection
                .prepare("SELECT count(*) FROM arrow(?) WHERE NOT json_valid(value)")?
                .query_row([&ArrowBatchRegistration::new(single)], |row| row.get(0))?;
            if invalid > 0 {
                return Err(DuckDbError::InvalidJson { column, invalid });
            }
        }
        Ok(())
    })
    .await
}
