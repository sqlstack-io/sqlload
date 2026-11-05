use std::sync::Arc;

use arrow_array::types::{Date32Type, Decimal128Type};
use arrow_array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, PrimitiveArray, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
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
        ParityFiles::FloatEdges => float_edges(),
        ParityFiles::TimestampEdges => timestamp_edges(),
        ParityFiles::DecimalEdges => decimal_edges(),
        ParityFiles::IntegerEdges => integer_edges(),
        ParityFiles::ComparisonSelfTest => Ok(Vec::new()),
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

/// NaN and both infinities, in both float widths, beside ordinary values.
fn float_edges() -> Result<Vec<(&'static str, RecordBatch)>, String> {
    let batch = RecordBatch::try_from_iter([
        ("id", array(Int64Array::from(vec![1, 2, 3, 4, 5, 6]))),
        (
            "wide",
            array(Float64Array::from(vec![
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(f64::NEG_INFINITY),
                Some(-0.0),
                Some(f64::MIN_POSITIVE),
                None,
            ])),
        ),
        (
            "narrow",
            array(Float32Array::from(vec![
                Some(f32::NAN),
                Some(f32::INFINITY),
                Some(f32::NEG_INFINITY),
                Some(1.1),
                Some(f32::MAX),
                None,
            ])),
        ),
    ])
    .map_err(|error| error.to_string())?;
    Ok(vec![("part-0.parquet", batch)])
}

/// Each timestamp unit, with sub-millisecond digits before and after 1970, with and without UTC.
fn timestamp_edges() -> Result<Vec<(&'static str, RecordBatch)>, String> {
    let micros = vec![
        Some(1_700_000_000_123_456),
        Some(1_700_000_000_123_457),
        Some(-1),
        Some(-1_500_001),
        Some(-62_135_596_800_000_000),
        None,
    ];
    let millis = vec![
        Some(1_700_000_000_123),
        Some(1_700_000_000_124),
        Some(-1),
        Some(-1_501),
        Some(0),
        None,
    ];
    let seconds = vec![
        Some(1_700_000_000),
        Some(1_700_000_001),
        Some(-1),
        Some(-1_501),
        Some(0),
        None,
    ];
    let batch = RecordBatch::try_from_iter([
        ("id", array(Int64Array::from(vec![1, 2, 3, 4, 5, 6]))),
        (
            "micros_zoned",
            array(TimestampMicrosecondArray::from(micros.clone()).with_timezone("UTC")),
        ),
        (
            "micros_wall",
            array(TimestampMicrosecondArray::from(micros)),
        ),
        (
            "millis_zoned",
            array(TimestampMillisecondArray::from(millis.clone()).with_timezone("UTC")),
        ),
        (
            "millis_wall",
            array(TimestampMillisecondArray::from(millis)),
        ),
        (
            "seconds_zoned",
            array(TimestampSecondArray::from(seconds.clone()).with_timezone("UTC")),
        ),
        ("seconds_wall", array(TimestampSecondArray::from(seconds))),
    ])
    .map_err(|error| error.to_string())?;
    Ok(vec![("part-0.parquet", batch)])
}

fn decimals(values: Vec<Option<i128>>, scale: i8) -> Result<ArrayRef, String> {
    let decimals = PrimitiveArray::<Decimal128Type>::from(values)
        .with_precision_and_scale(38, scale)
        .map_err(|error| error.to_string())?;
    Ok(array(decimals))
}

/// The largest and smallest values at precision 38, at scales 0, 10 and 37, and a precision 1 column.
fn decimal_edges() -> Result<Vec<(&'static str, RecordBatch)>, String> {
    const MAX: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;
    let small = PrimitiveArray::<Decimal128Type>::from(vec![Some(9), Some(-9), Some(0), None])
        .with_precision_and_scale(1, 0)
        .map_err(|error| error.to_string())?;
    let batch = RecordBatch::try_from_iter([
        ("id", array(Int64Array::from(vec![1, 2, 3, 4]))),
        (
            "whole",
            decimals(vec![Some(MAX), Some(-MAX), Some(0), None], 0)?,
        ),
        (
            "mixed",
            decimals(vec![Some(MAX), Some(-MAX), Some(1), None], 10)?,
        ),
        (
            "fraction",
            decimals(vec![Some(MAX), Some(-MAX), Some(1), None], 37)?,
        ),
        ("tiny", array(small)),
    ])
    .map_err(|error| error.to_string())?;
    Ok(vec![("part-0.parquet", batch)])
}

/// The minimum and maximum of each integer width the direct route allows.
fn integer_edges() -> Result<Vec<(&'static str, RecordBatch)>, String> {
    let batch = RecordBatch::try_from_iter([
        ("id", array(Int64Array::from(vec![1, 2, 3]))),
        (
            "i8",
            array(Int8Array::from(vec![Some(i8::MIN), Some(i8::MAX), None])),
        ),
        (
            "i16",
            array(Int16Array::from(vec![Some(i16::MIN), Some(i16::MAX), None])),
        ),
        (
            "i32",
            array(Int32Array::from(vec![Some(i32::MIN), Some(i32::MAX), None])),
        ),
        (
            "i64",
            array(Int64Array::from(vec![Some(i64::MIN), Some(i64::MAX), None])),
        ),
        (
            "u8",
            array(UInt8Array::from(vec![Some(u8::MIN), Some(u8::MAX), None])),
        ),
        (
            "u16",
            array(UInt16Array::from(vec![
                Some(u16::MIN),
                Some(u16::MAX),
                None,
            ])),
        ),
        (
            "u32",
            array(UInt32Array::from(vec![
                Some(u32::MIN),
                Some(u32::MAX),
                None,
            ])),
        ),
    ])
    .map_err(|error| error.to_string())?;
    Ok(vec![("part-0.parquet", batch)])
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
