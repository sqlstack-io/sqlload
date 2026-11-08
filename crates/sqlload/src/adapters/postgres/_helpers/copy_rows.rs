//! Arrow batches as a binary `COPY` stream, in the types of the staging table's columns: the mirror of the Postgres source's reader.

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Float64Type, Int16Type, Int64Type, Time64MicrosecondType,
    TimestampMicrosecondType,
};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{DataType, TimeUnit};
use bytes::{BufMut, Bytes, BytesMut};

use crate::column_types::models::ColumnType;
use crate::postgres_source::constants::{
    COPY_SIGNATURE, COPY_TRAILER, NULL_FIELD_LENGTH, NUMERIC_DIGIT_WIDTH, NUMERIC_NEGATIVE,
    NUMERIC_POSITIVE, POSTGRES_EPOCH_DAYS, POSTGRES_EPOCH_MICROS,
};
use crate::schema_plan::models::DesiredColumn;

/// How one staging column's values travel.
#[derive(Debug, Clone, Copy)]
enum Wire {
    Bool,
    Int2,
    Int8,
    Numeric { scale: u8 },
    Float8,
    Text,
    Date,
    Time,
    Timestamp,
    Bytea,
}

/// How a column of `column_type` travels, and the Arrow type its values are cast to first.
fn wire(column_type: &ColumnType) -> (Wire, DataType) {
    match column_type {
        ColumnType::Bool => (Wire::Bool, DataType::Boolean),
        ColumnType::SmallInt => (Wire::Int2, DataType::Int16),
        ColumnType::Int => (Wire::Int8, DataType::Int64),
        ColumnType::Decimal { precision, scale } => (
            Wire::Numeric { scale: *scale },
            DataType::Decimal128(*precision, i8::try_from(*scale).unwrap_or(i8::MAX)),
        ),
        ColumnType::Float => (Wire::Float8, DataType::Float64),
        ColumnType::Text
        | ColumnType::Varchar(_)
        | ColumnType::Json
        | ColumnType::Uuid
        | ColumnType::Native(_) => (Wire::Text, DataType::Utf8),
        ColumnType::Date => (Wire::Date, DataType::Date32),
        ColumnType::Time => (Wire::Time, DataType::Time64(TimeUnit::Microsecond)),
        ColumnType::TimestampTz => (
            Wire::Timestamp,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        ),
        ColumnType::TimestampNtz => (
            Wire::Timestamp,
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
        ColumnType::Binary => (Wire::Bytea, DataType::Binary),
    }
}

/// A `numeric` in Postgres's binary form: `value` scaled by `10^scale`, as base-10000 digits around the point.
pub(crate) fn numeric_bytes(value: i128, scale: u8) -> Vec<u8> {
    let width = NUMERIC_DIGIT_WIDTH as usize;
    let scale = usize::from(scale);
    let mut digits = value.unsigned_abs().to_string();
    if digits.len() <= scale {
        digits = format!("{}{digits}", "0".repeat(scale + 1 - digits.len()));
    }
    let (whole, fraction) = digits.split_at(digits.len() - scale);
    let whole = format!(
        "{}{whole}",
        "0".repeat((width - whole.len() % width) % width)
    );
    let fraction = format!(
        "{fraction}{}",
        "0".repeat((width - fraction.len() % width) % width)
    );
    let group = |text: &str| -> Vec<i16> {
        let mut groups: Vec<i16> = Vec::with_capacity(text.len() / width);
        for chunk in text.as_bytes().chunks(width) {
            let mut sum = 0i16;
            for digit in chunk {
                sum = sum * 10 + i16::from(digit - b'0');
            }
            groups.push(sum);
        }
        groups
    };
    let mut groups: Vec<i16> = group(&whole);
    let mut weight = i16::try_from(groups.len()).unwrap_or(i16::MAX) - 1;
    groups.extend(group(&fraction));
    let leading = groups.iter().take_while(|group| **group == 0).count();
    groups.drain(..leading);
    weight -= i16::try_from(leading).unwrap_or(i16::MAX);
    while groups.last() == Some(&0) {
        groups.pop();
    }
    if groups.is_empty() {
        weight = 0;
    }
    let sign = match value < 0 && !groups.is_empty() {
        true => NUMERIC_NEGATIVE,
        false => NUMERIC_POSITIVE,
    };
    let mut bytes: Vec<u8> = Vec::with_capacity(8 + groups.len() * 2);
    bytes.extend_from_slice(
        &i16::try_from(groups.len())
            .unwrap_or(i16::MAX)
            .to_be_bytes(),
    );
    bytes.extend_from_slice(&weight.to_be_bytes());
    bytes.extend_from_slice(&sign.to_be_bytes());
    bytes.extend_from_slice(&u16::try_from(scale).unwrap_or(u16::MAX).to_be_bytes());
    for group in groups {
        bytes.extend_from_slice(&group.to_be_bytes());
    }
    bytes
}

/// Field `row` of `array`, appended to `out` with its length; `-1` for NULL. A date or time before Postgres's range fails.
fn put_field(
    out: &mut BytesMut,
    (array, wire): (&ArrayRef, Wire),
    row: usize,
) -> Result<(), String> {
    if array.is_null(row) {
        out.put_i32(NULL_FIELD_LENGTH);
        return Ok(());
    }
    let out_of_range = || format!("row {row}: the value is out of Postgres's range");
    let mut fixed = |bytes: &[u8]| {
        out.put_i32(i32::try_from(bytes.len()).unwrap_or(i32::MAX));
        out.put_slice(bytes);
    };
    match wire {
        Wire::Bool => fixed(&[u8::from(array.as_boolean().value(row))]),
        Wire::Int2 => fixed(&array.as_primitive::<Int16Type>().value(row).to_be_bytes()),
        Wire::Int8 => fixed(&array.as_primitive::<Int64Type>().value(row).to_be_bytes()),
        Wire::Float8 => fixed(&array.as_primitive::<Float64Type>().value(row).to_be_bytes()),
        Wire::Numeric { scale } => fixed(&numeric_bytes(
            array.as_primitive::<Decimal128Type>().value(row),
            scale,
        )),
        Wire::Text => fixed(array.as_string::<i32>().value(row).as_bytes()),
        Wire::Bytea => fixed(array.as_binary::<i32>().value(row)),
        Wire::Date => fixed(
            &array
                .as_primitive::<Date32Type>()
                .value(row)
                .checked_sub(POSTGRES_EPOCH_DAYS)
                .ok_or_else(out_of_range)?
                .to_be_bytes(),
        ),
        Wire::Time => fixed(
            &array
                .as_primitive::<Time64MicrosecondType>()
                .value(row)
                .to_be_bytes(),
        ),
        Wire::Timestamp => fixed(
            &array
                .as_primitive::<TimestampMicrosecondType>()
                .value(row)
                .checked_sub(POSTGRES_EPOCH_MICROS)
                .ok_or_else(out_of_range)?
                .to_be_bytes(),
        ),
    }
    Ok(())
}

/// `batch`'s fields named by `columns`, in their order and cast to the types they travel as, with how each travels; a field `batch` lacks, or a value that does not fit its column, is an error, never a silent NULL. Fields not among `columns` are left out.
fn wire_columns(
    batch: &RecordBatch,
    columns: &[DesiredColumn],
) -> Result<Vec<(ArrayRef, Wire)>, String> {
    let options = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let schema = batch.schema();
    columns
        .iter()
        .map(|column| {
            let index = schema
                .index_of(&column.name)
                .map_err(|_| format!("column {} is not in the batch", column.name))?;
            let (wire, data_type) = wire(&column.column_type);
            let arrived = batch.column(index);
            let cast = cast_with_options(arrived, &data_type, &options).map_err(|error| {
                format!(
                    "column {} ({}) cannot be sent to Postgres as {}: {error}",
                    column.name,
                    arrived.data_type(),
                    column.column_type
                )
            })?;
            Ok((cast, wire))
        })
        .collect()
}

/// `batch` as one binary `COPY` stream into a table of `columns`, cut into pieces of about `chunk_bytes` at row boundaries.
pub(crate) fn copy_chunks(
    batch: &RecordBatch,
    columns: &[DesiredColumn],
    chunk_bytes: usize,
) -> Result<Vec<Bytes>, String> {
    let fields = wire_columns(batch, columns)?;
    let count = i16::try_from(fields.len())
        .map_err(|_| format!("{} columns are more than COPY takes", fields.len()))?;
    let mut chunks: Vec<Bytes> = Vec::new();
    let mut out = BytesMut::with_capacity(chunk_bytes.min(batch.get_array_memory_size() + 64));
    out.put_slice(COPY_SIGNATURE);
    out.put_i32(0);
    out.put_i32(0);
    for row in 0..batch.num_rows() {
        out.put_i16(count);
        for ((array, wire), column) in fields.iter().zip(columns) {
            put_field(&mut out, (array, *wire), row)
                .map_err(|reason| format!("column {}: {reason}", column.name))?;
        }
        if out.len() >= chunk_bytes {
            chunks.push(out.split().freeze());
        }
    }
    out.put_i16(COPY_TRAILER);
    chunks.push(out.freeze());
    Ok(chunks)
}
