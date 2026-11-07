//! Rows as the Storage Write API reads them: Arrow types it accepts, cut into requests it takes.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use prost::Message;

use crate::adapters::bigquery::_helpers::dialect::type_names::decimal_digits;
use crate::bigquery_api::main::arrow_request::arrow_request;
use crate::bigquery_api::models::AppendRowsRequest;
use crate::column_types::models::ColumnType;
use crate::schema_plan::models::DesiredColumn;

/// The Arrow type the Write API reads into a column of `column_type`, given the type the rows arrived as, or why it cannot load it yet.
fn write_api_type(column_type: &ColumnType, arrived: &DataType) -> Result<DataType, String> {
    let supported = match column_type {
        ColumnType::Bool => Some(DataType::Boolean),
        ColumnType::SmallInt | ColumnType::Int => Some(DataType::Int64),
        ColumnType::Decimal { precision, scale } => {
            let (precision, scale, big) = decimal_digits(*precision, *scale);
            let scale = i8::try_from(scale)
                .map_err(|_| format!("a decimal scale of {scale} has no Arrow type"))?;
            Some(match big {
                true => DataType::Decimal256(precision, scale),
                false => DataType::Decimal128(precision, scale),
            })
        }
        ColumnType::Float => Some(DataType::Float64),
        ColumnType::Text | ColumnType::Varchar(_) | ColumnType::Uuid | ColumnType::Json => {
            Some(DataType::Utf8)
        }
        ColumnType::Date => Some(DataType::Date32),
        ColumnType::Time => Some(DataType::Time64(TimeUnit::Microsecond)),
        ColumnType::TimestampTz => Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        )),
        ColumnType::TimestampNtz => Some(DataType::Timestamp(TimeUnit::Microsecond, None)),
        ColumnType::Binary => Some(DataType::Binary),
        ColumnType::Native(_) => (!arrived.is_nested()).then(|| arrived.clone()),
    };
    supported.ok_or_else(|| "nested values are not streamed".to_owned())
}

/// `batch` in the Arrow types the Write API reads for `columns`, matched by name, every field nullable and without metadata; a value that does not fit its column is an error, never a silent NULL.
pub(crate) fn write_api_batch(
    batch: &RecordBatch,
    columns: &[DesiredColumn],
) -> Result<RecordBatch, String> {
    let options = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let mut fields: Vec<Field> = Vec::with_capacity(batch.num_columns());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        let name = field.name();
        let column = columns
            .iter()
            .find(|column| &column.name == name)
            .ok_or_else(|| format!("column {name} is not among the table's planned columns"))?;
        let target = write_api_type(&column.column_type, field.data_type()).map_err(|reason| {
            format!(
                "column {name} ({}) cannot load into BigQuery yet: {reason}",
                field.data_type()
            )
        })?;
        let cast = cast_with_options(array, &target, &options).map_err(|error| {
            format!("column {name} cannot be sent to BigQuery as {target}: {error}")
        })?;
        fields.push(Field::new(name, target, true));
        arrays.push(cast);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|error| error.to_string())
}

/// `batch` cut into append requests of at most `max_bytes` each, every one carrying the writer schema and no stream yet, with its rows; a single row larger than `max_bytes` goes alone, for BigQuery to refuse by name.
pub(crate) fn append_requests(
    batch: &RecordBatch,
    max_bytes: usize,
) -> Result<Vec<(AppendRowsRequest, usize)>, String> {
    let rows = batch.num_rows();
    let memory = batch.get_array_memory_size().max(1);
    let mut step = (rows.saturating_mul(max_bytes) / memory).clamp(1, rows.max(1));
    let mut requests: Vec<(AppendRowsRequest, usize)> = Vec::new();
    let mut start = 0;
    while start < rows {
        let length = step.min(rows - start);
        let request = arrow_request("", 0, &batch.slice(start, length), true)
            .map_err(|error| error.to_string())?;
        if request.encoded_len() > max_bytes && length > 1 {
            step = length / 2;
            continue;
        }
        requests.push((request, length));
        start += length;
    }
    Ok(requests)
}
