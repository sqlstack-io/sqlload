use std::sync::Arc;

use arrow_array::types::{Date32Type, Decimal128Type};
use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int16Array, Int64Array, PrimitiveArray, RecordBatch,
    StringArray, TimestampMicrosecondArray,
};
use bytes::Bytes;

use crate::parity::models::ParityFiles;

/// The case's files: each key and its rows.
pub(crate) fn case_files(files: ParityFiles) -> Result<Vec<(&'static str, RecordBatch)>, String> {
    match files {
        ParityFiles::EveryType => every_type(),
        ParityFiles::Hashed => hashed_values(),
        ParityFiles::Json => json_values(),
        ParityFiles::BadJson => bad_json(),
        ParityFiles::Partitioned => partitioned(),
        ParityFiles::BadPartition => bad_partition(),
    }
}

fn array<T: arrow_array::Array + 'static>(array: T) -> ArrayRef {
    Arc::new(array)
}

/// Every candidate type, with NULLs and edge values.
fn every_type() -> Result<Vec<(&'static str, RecordBatch)>, String> {
    let decimals = PrimitiveArray::<Decimal128Type>::from(vec![
        Some(123_456_789_012_345_678),
        Some(-1),
        Some(0),
        None,
    ])
    .with_precision_and_scale(18, 2)
    .map_err(|error| error.to_string())?;
    let batch = RecordBatch::try_from_iter([
        ("id", array(Int64Array::from(vec![1, 2, 3, 4]))),
        (
            "flag",
            array(BooleanArray::from(vec![
                Some(true),
                Some(false),
                None,
                Some(true),
            ])),
        ),
        (
            "small",
            array(Int16Array::from(vec![
                Some(32767),
                Some(-32768),
                Some(0),
                None,
            ])),
        ),
        (
            "big",
            array(Int64Array::from(vec![
                Some(i64::MAX),
                Some(i64::MIN),
                Some(0),
                None,
            ])),
        ),
        ("amount", array(decimals)),
        (
            "ratio",
            array(Float64Array::from(vec![
                Some(1.5),
                Some(-0.000_001),
                Some(1e300),
                None,
            ])),
        ),
        (
            "note",
            array(StringArray::from(vec![
                Some("plain"),
                Some(""),
                Some("Ünïcødé 日本 o'brien \\\\ x"),
                None,
            ])),
        ),
        (
            "day",
            array(PrimitiveArray::<Date32Type>::from(vec![
                Some(20_000),
                Some(0),
                Some(-1),
                None,
            ])),
        ),
        (
            "at_zoned",
            array(
                TimestampMicrosecondArray::from(vec![
                    Some(1_700_000_000_123_456),
                    Some(0),
                    Some(-1),
                    None,
                ])
                .with_timezone("UTC"),
            ),
        ),
        (
            "at_wall",
            array(TimestampMicrosecondArray::from(vec![
                Some(1_700_000_000_123_456),
                Some(0),
                Some(-1),
                None,
            ])),
        ),
    ])
    .map_err(|error| error.to_string())?;
    Ok(vec![("part-0.parquet", batch)])
}

fn ids_and_text(values: Vec<Option<&'static str>>) -> Result<RecordBatch, String> {
    let ids: Vec<i64> = (1..=values.len() as i64).collect();
    RecordBatch::try_from_iter([
        ("id", array(Int64Array::from(ids))),
        ("note", array(StringArray::from(values))),
    ])
    .map_err(|error| error.to_string())
}

/// Values whose digests must match byte for byte.
fn hashed_values() -> Result<Vec<(&'static str, RecordBatch)>, String> {
    Ok(vec![(
        "part-0.parquet",
        ids_and_text(vec![
            Some(""),
            Some("abc"),
            Some("é"),
            Some("Ünïcødé 日本"),
            Some("o'brien \\\\ x"),
            None,
        ])?,
    )])
}

/// Text that parses as JSON of every shape.
fn json_values() -> Result<Vec<(&'static str, RecordBatch)>, String> {
    Ok(vec![(
        "part-0.parquet",
        ids_and_text(vec![
            Some("{\"a\": 1, \"b\": [1, 2.5, null]}"),
            Some("[1, \"x\"]"),
            Some("\"text\""),
            Some("12.50"),
            Some("null"),
            None,
        ])?,
    )])
}

/// Text that does not parse as JSON.
fn bad_json() -> Result<Vec<(&'static str, RecordBatch)>, String> {
    Ok(vec![(
        "part-0.parquet",
        ids_and_text(vec![Some("{\"a\": 1}"), Some("{not json")])?,
    )])
}

/// Files in typed partitions, of different lengths, for partition values and row numbers.
fn partitioned() -> Result<Vec<(&'static str, RecordBatch)>, String> {
    Ok(vec![
        (
            "day=2026-11-01/n=7/label=a/part-0.parquet",
            ids_and_text(vec![Some("x"), Some("y"), Some("z")])?,
        ),
        (
            "day=2026-11-01/n=7/label=a/part-1.parquet",
            ids_and_text(vec![Some("w")])?,
        ),
        (
            "day=2026-11-02/n=-1/label=b/part-0.parquet",
            ids_and_text(vec![Some("v"), None])?,
        ),
    ])
}

/// A partition value that is not a date.
fn bad_partition() -> Result<Vec<(&'static str, RecordBatch)>, String> {
    Ok(vec![(
        "day=not-a-date/part-0.parquet",
        ids_and_text(vec![Some("x")])?,
    )])
}

/// `batch` as a Parquet file.
pub(crate) fn parquet_file(batch: &RecordBatch) -> Result<Bytes, String> {
    let mut buffer: Vec<u8> = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buffer, batch.schema(), None)
        .map_err(|error| error.to_string())?;
    writer.write(batch).map_err(|error| error.to_string())?;
    writer.close().map_err(|error| error.to_string())?;
    Ok(Bytes::from(buffer))
}
