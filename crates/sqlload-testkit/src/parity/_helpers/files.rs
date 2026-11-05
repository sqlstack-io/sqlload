use std::sync::Arc;

use arrow_array::types::{Date32Type, Decimal128Type, Decimal256Type, Int16Type, Int32Type};
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date64Array, DictionaryArray, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, PrimitiveArray,
    RecordBatch, StringArray, Time32MillisecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use bytes::Bytes;

/// A 256-bit decimal's integer.
type Wide = <Decimal256Type as arrow_array::ArrowPrimitiveType>::Native;

use crate::parity::models::ParityFiles;

/// The case's files: each key and its Parquet bytes, with or without the embedded Arrow schema.
pub(crate) fn case_files(
    files: ParityFiles,
    embedded_schema: bool,
) -> Result<Vec<(&'static str, Bytes)>, String> {
    match files {
        ParityFiles::Int96Timestamps => return Ok(vec![("part-0.parquet", int96_file()?)]),
        ParityFiles::NdjsonValues => {
            return Ok(vec![("part-0.ndjson", Bytes::from(NDJSON_VALUES))]);
        }
        ParityFiles::JsonLayouts => {
            return Ok(vec![
                ("array.json", Bytes::from(JSON_ARRAY)),
                ("lines.json", Bytes::from(JSON_OBJECTS)),
            ]);
        }
        ParityFiles::CsvValues => {
            return Ok(vec![
                ("part-0.csv", Bytes::from(CSV_VALUES)),
                ("part-1.csv", Bytes::from(CSV_REORDERED)),
            ]);
        }
        ParityFiles::CsvLateMisfit => {
            let mut text = String::from("id,qty\n");
            for id in 1..=10_000 {
                text.push_str(&format!("{id},{id}\n"));
            }
            text.push_str("10001,x\n");
            return Ok(vec![("part-0.csv", Bytes::from(text))]);
        }
        ParityFiles::LateKey => {
            return Ok(vec![(
                "part-0.ndjson",
                past_sample(r#""qty":1,"coupon":"X1""#),
            )]);
        }
        ParityFiles::LateMisfit => {
            return Ok(vec![("part-0.ndjson", past_sample(r#""qty":"x""#))]);
        }
        _ => {}
    }
    let batches = match files {
        ParityFiles::NanoTimestamps => nano_timestamps(),
        ParityFiles::StoredTypes => stored_types(),
        ParityFiles::Int96Timestamps
        | ParityFiles::NdjsonValues
        | ParityFiles::JsonLayouts
        | ParityFiles::CsvValues
        | ParityFiles::CsvLateMisfit
        | ParityFiles::LateKey
        | ParityFiles::LateMisfit => Ok(Vec::new()),
        ParityFiles::EveryType => every_type(),
        ParityFiles::Hashed => hashed_values(),
        ParityFiles::Json => json_values(),
        ParityFiles::BadJson => bad_json(),
        ParityFiles::Partitioned => partitioned(),
        ParityFiles::BadPartition => bad_partition(),
        ParityFiles::FloatEdges => float_edges(),
        ParityFiles::TimestampEdges => timestamp_edges(),
        ParityFiles::OffsetTimestamps => offset_timestamps(),
        ParityFiles::DecimalEdges => decimal_edges(),
        ParityFiles::IntegerEdges => integer_edges(),
        ParityFiles::ComparisonSelfTest => Ok(Vec::new()),
    }?;
    batches
        .into_iter()
        .map(|(key, batch)| Ok((key, parquet_file(&batch, embedded_schema)?)))
        .collect()
}

/// Nanoseconds since the epoch: below the microsecond before and after 1970, one nanosecond either side of it, and a NULL.
const NANO_INSTANTS: [Option<i64>; 6] = [
    Some(-1_500_000_000_123_456_789),
    Some(-1),
    Some(0),
    Some(1),
    Some(1_700_000_000_987_654_321),
    None,
];

/// Nanosecond timestamps and times.
fn nano_timestamps() -> Result<Vec<(&'static str, RecordBatch)>, String> {
    let instants = || TimestampNanosecondArray::from(NANO_INSTANTS.to_vec());
    let batch = RecordBatch::try_from_iter([
        ("id", array(Int64Array::from(vec![1, 2, 3, 4, 5, 6]))),
        ("at_wall", array(instants())),
        ("at_utc", array(instants().with_timezone("UTC"))),
        ("at_zoned", array(instants().with_timezone("+01:00"))),
        (
            "time_of_day",
            array(Time64NanosecondArray::from(vec![
                Some(1),
                Some(999),
                Some(1_000),
                Some(43_200_123_456_789),
                Some(86_399_999_999_999),
                None,
            ])),
        ),
    ])
    .map_err(|error| error.to_string())?;
    Ok(vec![("part-0.parquet", batch)])
}

/// Types whose files need more than a plain cast, or were refused before.
fn stored_types() -> Result<Vec<(&'static str, RecordBatch)>, String> {
    let names = DictionaryArray::<Int32Type>::from_iter([Some("a"), Some("b"), Some("a"), None]);
    let codes: DictionaryArray<Int16Type> = vec![Some("x"), None, Some("x"), Some("y")]
        .into_iter()
        .collect();
    let wide = PrimitiveArray::<Decimal256Type>::from(vec![
        Some(Wide::from_i128(1_500)),
        Some(Wide::from_i128(
            -99_999_999_999_999_999_999_999_999_999_999_999_999,
        )),
        Some(Wide::from_i128(0)),
        None,
    ])
    .with_precision_and_scale(38, 3)
    .map_err(|error| error.to_string())?;
    let batch = RecordBatch::try_from_iter([
        ("id", array(Int64Array::from(vec![1, 2, 3, 4]))),
        (
            "at_seconds",
            array(
                TimestampSecondArray::from(vec![
                    Some(-1_500_000_000),
                    Some(0),
                    Some(1_700_000_000),
                    None,
                ])
                .with_timezone("UTC"),
            ),
        ),
        (
            "wall_seconds",
            array(TimestampSecondArray::from(vec![
                Some(-1),
                Some(0),
                Some(253_402_300_799),
                None,
            ])),
        ),
        (
            "day64",
            array(Date64Array::from(vec![
                Some(-86_400_000 * 400),
                Some(0),
                Some(86_400_000 * 20_000),
                None,
            ])),
        ),
        (
            "unsigned",
            array(UInt64Array::from(vec![
                Some(0),
                Some(u64::MAX),
                Some(1),
                None,
            ])),
        ),
        ("names", array(names)),
        ("codes", array(codes)),
        (
            "bytes",
            array(BinaryArray::from(vec![
                Some(&b"\x00\xffab"[..]),
                Some(&b""[..]),
                Some(&b"plain"[..]),
                None,
            ])),
        ),
        (
            "fixed",
            array(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    vec![
                        Some(b"0123456789abcdef".to_vec()),
                        Some(vec![0; 16]),
                        None,
                        Some(vec![255; 16]),
                    ]
                    .into_iter(),
                    16,
                )
                .map_err(|error| error.to_string())?,
            ),
        ),
        (
            "millis_of_day",
            array(Time32MillisecondArray::from(vec![
                Some(0),
                Some(86_399_999),
                Some(1),
                None,
            ])),
        ),
        (
            "micros_of_day",
            array(Time64MicrosecondArray::from(vec![
                Some(1),
                Some(86_399_999_999),
                Some(0),
                None,
            ])),
        ),
        ("wide", array(wide)),
    ])
    .map_err(|error| error.to_string())?;
    Ok(vec![("part-0.parquet", batch)])
}

/// An `id` and an INT96 timestamp column, written with the low-level writer since the Arrow writer has no INT96.
fn int96_file() -> Result<Bytes, String> {
    use parquet::data_type::{Int64Type as ParquetInt64, Int96, Int96Type};
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::parser::parse_message_type;
    let failed = |error: parquet::errors::ParquetError| error.to_string();
    let schema = Arc::new(
        parse_message_type("message m { required int64 id; optional int96 at; }")
            .map_err(failed)?,
    );
    let present: Vec<Int96> = NANO_INSTANTS
        .iter()
        .flatten()
        .map(|nanos| {
            let day = nanos.div_euclid(NANOS_PER_DAY);
            let within = nanos.rem_euclid(NANOS_PER_DAY);
            let mut value = Int96::new();
            value.set_data(
                (within & 0xFFFF_FFFF) as u32,
                (within >> 32) as u32,
                (day + JULIAN_DAY_OF_EPOCH) as u32,
            );
            value
        })
        .collect();
    let levels: Vec<i16> = NANO_INSTANTS
        .iter()
        .map(|nanos| i16::from(nanos.is_some()))
        .collect();
    let ids: Vec<i64> = (1..=NANO_INSTANTS.len() as i64).collect();
    let mut buffer: Vec<u8> = Vec::new();
    let mut writer =
        SerializedFileWriter::new(&mut buffer, schema, Arc::new(WriterProperties::default()))
            .map_err(failed)?;
    let mut group = writer.next_row_group().map_err(failed)?;
    let mut column = group.next_column().map_err(failed)?.ok_or("no id column")?;
    column
        .typed::<ParquetInt64>()
        .write_batch(&ids, None, None)
        .map_err(failed)?;
    column.close().map_err(failed)?;
    let mut column = group.next_column().map_err(failed)?.ok_or("no at column")?;
    column
        .typed::<Int96Type>()
        .write_batch(&present, Some(&levels), None)
        .map_err(failed)?;
    column.close().map_err(failed)?;
    group.close().map_err(failed)?;
    writer.close().map_err(failed)?;
    Ok(Bytes::from(buffer))
}

const NANOS_PER_DAY: i64 = 86_400_000_000_000;
/// The Julian day number of 1970-01-01, which INT96 counts days from.
const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;

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

/// Instants in microseconds and milliseconds: sub-millisecond digits, before and after 1970, and a NULL.
fn edge_instants() -> [Vec<Option<i64>>; 2] {
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
    [micros, millis]
}

/// Each timestamp unit the direct route allows, in the Arrow zone `+00:00`.
fn offset_timestamps() -> Result<Vec<(&'static str, RecordBatch)>, String> {
    let [micros, millis] = edge_instants();
    let batch = RecordBatch::try_from_iter([
        ("id", array(Int64Array::from(vec![1, 2, 3, 4, 5, 6]))),
        (
            "micros",
            array(TimestampMicrosecondArray::from(micros).with_timezone("+00:00")),
        ),
        (
            "millis",
            array(TimestampMillisecondArray::from(millis).with_timezone("+00:00")),
        ),
    ])
    .map_err(|error| error.to_string())?;
    Ok(vec![("part-0.parquet", batch)])
}

/// Each timestamp unit the direct route allows, with sub-millisecond digits before and after 1970, with and without UTC.
fn timestamp_edges() -> Result<Vec<(&'static str, RecordBatch)>, String> {
    let [micros, millis] = edge_instants();
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

/// `batch` as a Parquet file, with or without the embedded Arrow schema.
pub(crate) fn parquet_file(batch: &RecordBatch, embedded_schema: bool) -> Result<Bytes, String> {
    let options = parquet::arrow::arrow_writer::ArrowWriterOptions::new()
        .with_skip_arrow_metadata(!embedded_schema);
    let mut buffer: Vec<u8> = Vec::new();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new_with_options(&mut buffer, batch.schema(), options)
            .map_err(|error| error.to_string())?;
    writer.write(batch).map_err(|error| error.to_string())?;
    writer.close().map_err(|error| error.to_string())?;
    Ok(Bytes::from(buffer))
}

/// NDJSON of every inferred type: 64-bit integer edges, floats from integers and exponents, booleans, text from strings, numbers and booleans, RFC 3339 timestamps with offsets before and after 1970 and digits below the microsecond, a key given twice, objects and arrays, NULLs and missing keys.
const NDJSON_VALUES: &str = concat!(
    r#"{"id":1,"n":1,"f":1.5,"b":true,"s":"plain","ts":"2026-11-05T12:00:00.123456Z","o":{"x":1,"y":[1,2]},"mixed":"007","u":"Ünïcødé \"q\" \\ x"}"#,
    "\n",
    r#"{"id":2,"n":-9223372036854775808,"f":3,"b":false,"s":"","ts":"1969-12-31T23:59:59.999999-02:00","o":[1,{"a":null}],"mixed":7,"u":null}"#,
    "\n",
    r#"{"id":3,"n":null,"f":-0.25,"b":null,"ts":null,"o":null,"mixed":true}"#,
    "\n",
    r#"{"id":4,"n":9223372036854775807,"f":2e10,"s":"x","ts":"2000-01-01T00:00:00+05:30","o":{},"mixed":"text"}"#,
    "\n",
    r#"{"id":5,"s":"first","ts":"2026-01-01T00:00:00.1234567Z","s":"last"}"#,
    "\n",
    r#"{"id":6,"ts":"1969-12-31T23:59:59.9999999Z"}"#,
    "\n",
);

/// A `json` file holding one top-level array of objects.
const JSON_ARRAY: &str = r#"[{"id":1,"v":"a"},{"id":2,"v":"b"}]"#;

/// A `json` file holding objects one after another, one over two lines.
const JSON_OBJECTS: &str = "{\"id\":3,\"v\":\"c\"}\n{\"id\":4,\n\"v\":\"d\"}\n";

/// NDJSON with more records than sampling reads, `id` and `qty` ints, the last record's keys after `id` being `last`.
fn past_sample(last: &str) -> Bytes {
    let mut text = String::new();
    for id in 1..=10_000 {
        text.push_str(&format!("{{\"id\":{id},\"qty\":{id}}}\n"));
    }
    text.push_str(&format!("{{\"id\":10001,{last}}}\n"));
    Bytes::from(text)
}

/// CSV of every inferred type: 64-bit integer edges, floats from integers and exponents, booleans in any case, RFC 3339 timestamps with offsets and digits below the microsecond, text that looks like numbers, quoted delimiters, quotes, backslashes, line breaks and spaces, and empty fields, quoted or not, as NULL.
const CSV_VALUES: &str = concat!(
    "id,n,f,b,s,ts,mixed,u\n",
    "1,1,1.5,true,plain,2026-11-05T12:00:00.123456Z,007,\"Ünïcødé \"\"q\"\" \\ x\"\n",
    "2,-9223372036854775808,3,FALSE,,1969-12-31T23:59:59.999999-02:00,7,\n",
    "3,,-0.25,,\"with, comma\",,True,\"line\nbreak\"\n",
    "4,9223372036854775807,2e10,false,x,2000-01-01T00:00:00+05:30,text,\"   spaced  \"\n",
    "5,\"\",\"\",\"\",\"\",2026-01-01T00:00:00.1234567Z,\"\",\"\"\n",
    "6,,,,,1969-12-31T23:59:59.9999999Z,,\n",
);

/// A CSV file whose header puts the columns in another order and lacks some.
const CSV_REORDERED: &str = "u,id,s\nz,5,y\n";
