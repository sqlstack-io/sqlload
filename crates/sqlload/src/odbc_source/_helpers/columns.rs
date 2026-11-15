//! Which columns of a read are too long to bind, and the buffer the others take per row.

use arrow_odbc::odbc_api::ResultSetMetadata;
use arrow_odbc::odbc_api::buffers::BufferDesc;
use arrow_schema::{DataType, Schema, TimeUnit};

use crate::odbc_source::constants::{DEFAULT_LONG_COLUMN_THRESHOLD, TIME_TEXT_LENGTHS};
use crate::odbc_source::errors::OdbcSourceError;
use crate::odbc_source::models::{FetchShape, FetchSize, ValueLimits};

/// The shape of a read of `schema`'s columns (as `fetch_schema` fetches them), described by `metadata`: a text or binary column is long when the buffer per value arrow-odbc would bind for it (its described size in bytes, at most `max_text_size` or `max_binary_size`) passes the threshold, or when it has no size; the others' buffers, as arrow-odbc sizes them, make the bytes per row. Text is sized as arrow-odbc reads it: UTF-16 on Windows, UTF-8 elsewhere.
pub(crate) fn fetch_shape(
    metadata: &mut impl ResultSetMetadata,
    schema: &Schema,
    limits: &ValueLimits,
) -> Result<FetchShape, OdbcSourceError> {
    let threshold = limits
        .long_column_threshold
        .unwrap_or(DEFAULT_LONG_COLUMN_THRESHOLD);
    let wide = cfg!(target_os = "windows");
    let mut shape = FetchShape {
        bound_bytes_per_row: 0,
        long_columns: Vec::new(),
        columns: schema.fields().len(),
    };
    for (field, number) in schema.fields().iter().zip(1u16..) {
        let nullable = field.is_nullable();
        let buffer = match field.data_type() {
            DataType::Utf8 => {
                let sql_type = metadata.col_data_type(number)?;
                let length = match wide {
                    true => sql_type.utf16_len(),
                    false => sql_type.utf8_len(),
                };
                let length = match length {
                    Some(length) => Some(length.get()),
                    None => metadata.col_display_size(number)?.map(|size| size.get()),
                };
                capped(length, limits.max_text_size)
                    .filter(|&length| length.saturating_mul(if wide { 2 } else { 1 }) <= threshold)
                    .map(|max_str_len| match wide {
                        true => BufferDesc::WText { max_str_len },
                        false => BufferDesc::Text { max_str_len },
                    })
            }
            DataType::Binary => {
                let length = metadata.col_data_type(number)?.column_size();
                capped(length.map(|length| length.get()), limits.max_binary_size)
                    .filter(|&length| length <= threshold)
                    .map(|max_bytes| BufferDesc::Binary { max_bytes })
            }
            other => Some(bound_buffer(other, nullable)),
        };
        match buffer {
            Some(buffer) => shape.bound_bytes_per_row += buffer.bytes_per_row(),
            None => shape.long_columns.push(field.name().clone()),
        }
    }
    if !shape.long_columns.is_empty() {
        shape.bound_bytes_per_row = 0;
    }
    Ok(shape)
}

/// The rows one fetch of `shape` reads under `budget` bytes, at most `batch_rows` and at least one, and the bytes it holds: its bound buffers, or for a table read value by value the budget, since its values count towards it.
pub(crate) fn fetch_size(shape: &FetchShape, budget: usize, batch_rows: usize) -> FetchSize {
    let batch_rows = batch_rows.max(1);
    match (shape.long_columns.is_empty(), shape.bound_bytes_per_row) {
        (false, _) => FetchSize {
            rows: batch_rows,
            bytes: budget,
        },
        (true, 0) => FetchSize {
            rows: batch_rows,
            bytes: 0,
        },
        (true, per_row) => {
            let rows = (budget / per_row).clamp(1, batch_rows);
            FetchSize {
                rows,
                bytes: rows.saturating_mul(per_row),
            }
        }
    }
}

/// A described size capped by its setting; `None` when neither gives one.
fn capped(length: Option<usize>, cap: Option<usize>) -> Option<usize> {
    match (length, cap) {
        (Some(length), Some(cap)) => Some(length.min(cap)),
        (length, cap) => length.or(cap),
    }
}

/// The buffer arrow-odbc binds for a column of a type other than text or binary.
fn bound_buffer(data_type: &DataType, nullable: bool) -> BufferDesc {
    match data_type {
        DataType::Boolean => BufferDesc::Bit { nullable },
        DataType::Int8 => BufferDesc::I8 { nullable },
        DataType::Int16 => BufferDesc::I16 { nullable },
        DataType::Int32 => BufferDesc::I32 { nullable },
        DataType::UInt8 => BufferDesc::U8 { nullable },
        DataType::Float32 => BufferDesc::F32 { nullable },
        DataType::Float64 => BufferDesc::F64 { nullable },
        DataType::Date32 => BufferDesc::Date { nullable },
        DataType::Time32(TimeUnit::Second) => BufferDesc::Time { nullable },
        DataType::Time32(_) => BufferDesc::Text {
            max_str_len: TIME_TEXT_LENGTHS[0],
        },
        DataType::Time64(TimeUnit::Microsecond) => BufferDesc::Text {
            max_str_len: TIME_TEXT_LENGTHS[1],
        },
        DataType::Time64(_) => BufferDesc::Text {
            max_str_len: TIME_TEXT_LENGTHS[2],
        },
        DataType::Timestamp(..) => BufferDesc::Timestamp { nullable },
        DataType::FixedSizeBinary(length) => BufferDesc::Binary {
            max_bytes: usize::try_from(*length).unwrap_or_default(),
        },
        _ => BufferDesc::I64 { nullable },
    }
}
