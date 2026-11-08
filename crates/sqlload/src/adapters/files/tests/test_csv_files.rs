use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int64Array, RecordBatch, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema};
use flate2::read::GzDecoder;

use crate::adapters::files::tests::helpers::csv_file;
use crate::adapters::files::tests::test_types::{CsvCompressionTestCase, CsvRenderTestCase};
use crate::column_types::constants::ARROW_TYPE_METADATA_KEY;
use crate::project::models::{CsvOutput, FilesFormat, OutputCompression, OutputEncoding};

/// 2026-11-04 00:00:00.000001 UTC, in microseconds.
const NOV_4_PLUS_1_US: i64 = 1_793_750_400_000_001;
/// 2026-11-04 as days since 1970-01-01.
const NOV_4_DAYS: i32 = 20_761;
/// 01:02:03.000001 in microseconds.
const ONE_TWO_THREE: i64 = 3_723_000_001;
/// The gzip magic number.
const GZIP_MAGIC: &[u8] = &[0x1f, 0x8b];
/// The zstd frame magic number.
const ZSTD_MAGIC: &[u8] = &[0x28, 0xb5, 0x2f, 0xfd];
/// Text padding each row of the compression test to about 100 bytes.
const PADDING: &str =
    "................................................................................";

fn column(name: &str, array: ArrayRef) -> (Field, ArrayRef) {
    (Field::new(name, array.data_type().clone(), true), array)
}

fn json_column(name: &str, values: Vec<Option<&str>>) -> (Field, ArrayRef) {
    let field = Field::new(name, DataType::Utf8, true).with_metadata(HashMap::from([(
        ARROW_TYPE_METADATA_KEY.to_owned(),
        "json".to_owned(),
    )]));
    (field, Arc::new(StringArray::from(values)))
}

fn instants(zone: Option<&str>) -> ArrayRef {
    Arc::new(
        TimestampMicrosecondArray::from(vec![Some(NOV_4_PLUS_1_US), None])
            .with_timezone_opt(zone.map(str::to_owned)),
    )
}

fn tab_separated() -> CsvOutput {
    CsvOutput {
        delimiter: '\t',
        header: false,
        quote: '"',
        null_value: "NULL".to_owned(),
    }
}

fn csv_text(test_case: &CsvRenderTestCase) -> Result<String, String> {
    let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = test_case.columns.iter().cloned().unzip();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|error| error.to_string())?;
    let encoding = OutputEncoding {
        format: FilesFormat::Csv,
        csv: test_case.csv.clone(),
        ..OutputEncoding::default()
    };
    let bytes = csv_file(&[batch], &encoding)?;
    String::from_utf8(bytes).map_err(|error| error.to_string())
}

fn decompressed(compression: OutputCompression, bytes: &[u8]) -> Result<Vec<u8>, String> {
    let mut text = Vec::new();
    match compression {
        OutputCompression::None => text.extend_from_slice(bytes),
        OutputCompression::Gzip => {
            GzDecoder::new(bytes)
                .read_to_end(&mut text)
                .map_err(|error| error.to_string())?;
        }
        OutputCompression::Zstd => {
            text = zstd::decode_all(bytes).map_err(|error| error.to_string())?;
        }
    }
    Ok(text)
}

#[test]
fn given_typed_columns_when_writing_csv_then_each_value_renders_as_documented() {
    let test_cases = [
        CsvRenderTestCase {
            description: "text is quoted only when it must be, and NULL stays apart from empty text",
            columns: vec![column(
                "name",
                Arc::new(StringArray::from(vec![
                    Some("plain"),
                    Some(""),
                    None,
                    Some("a,b"),
                    Some("say \"hi\""),
                    Some("two\nlines"),
                ])),
            )],
            csv: CsvOutput::default(),
            expected_text: Ok("name\nplain\n\"\"\n\n\"a,b\"\n\"say \"\"hi\"\"\"\n\"two\nlines\"\n"),
        },
        CsvRenderTestCase {
            description: "a tab delimiter, no header, and a null_value that text equal to it is quoted against",
            columns: vec![
                column(
                    "word",
                    Arc::new(StringArray::from(vec![Some("NULL"), None])),
                ),
                column("n", Arc::new(Int64Array::from(vec![Some(-7), None]))),
            ],
            csv: tab_separated(),
            expected_text: Ok("\"NULL\"\t-7\nNULL\tNULL\n"),
        },
        CsvRenderTestCase {
            description: "a header name holding the delimiter is quoted",
            columns: vec![column("a,b", Arc::new(Int64Array::from(vec![1])))],
            csv: CsvOutput::default(),
            expected_text: Ok("\"a,b\"\n1\n"),
        },
        CsvRenderTestCase {
            description: "doubles are plain decimals without exponents, with named non-finite values",
            columns: vec![column(
                "x",
                Arc::new(Float64Array::from(vec![
                    0.1,
                    1e21,
                    1e-7,
                    -0.0,
                    f64::NAN,
                    f64::INFINITY,
                    f64::NEG_INFINITY,
                ])),
            )],
            csv: CsvOutput::default(),
            expected_text: Ok(
                "x\n0.1\n1000000000000000000000\n0.0000001\n-0\nNaN\nInfinity\n-Infinity\n",
            ),
        },
        CsvRenderTestCase {
            description: "single-precision floats keep their own shortest digits",
            columns: vec![column(
                "x",
                Arc::new(Float32Array::from(vec![0.1_f32, 16_777_216.0])),
            )],
            csv: CsvOutput::default(),
            expected_text: Ok("x\n0.1\n16777216\n"),
        },
        CsvRenderTestCase {
            description: "timestamps with a zone are written in UTC with Z; without one, as they are",
            columns: vec![
                column("utc", instants(Some("UTC"))),
                column("paris", instants(Some("+02:00"))),
                column("ntz", instants(None)),
                column(
                    "day",
                    Arc::new(Date32Array::from(vec![Some(NOV_4_DAYS), None])),
                ),
                column(
                    "at",
                    Arc::new(Time64MicrosecondArray::from(vec![
                        Some(ONE_TWO_THREE),
                        None,
                    ])),
                ),
            ],
            csv: CsvOutput::default(),
            expected_text: Ok(
                "utc,paris,ntz,day,at\n2026-11-04T00:00:00.000001Z,2026-11-04T00:00:00.000001Z,2026-11-04T00:00:00.000001,2026-11-04,01:02:03.000001\n,,,,\n",
            ),
        },
        CsvRenderTestCase {
            description: "booleans, decimals at their scale, and binary as lowercase hex",
            columns: vec![
                column("ok", Arc::new(BooleanArray::from(vec![true, false]))),
                column(
                    "amount",
                    Arc::new(
                        Decimal128Array::from(vec![12_345_i128, -5])
                            .with_precision_and_scale(10, 2)
                            .unwrap_or_else(|error| panic!("{error}")),
                    ),
                ),
                column(
                    "raw",
                    Arc::new(BinaryArray::from(vec![&[0xde_u8, 0xad][..], &[][..]])),
                ),
            ],
            csv: CsvOutput::default(),
            expected_text: Ok("ok,amount,raw\ntrue,123.45,dead\nfalse,-0.05,\"\"\n"),
        },
        CsvRenderTestCase {
            description: "JSON is compacted outside its strings and quoted like any text",
            columns: vec![json_column(
                "doc",
                vec![Some("{ \"a\" : [1, 2],\n \"b\": \"x y\" }"), Some(" 5 ")],
            )],
            csv: CsvOutput::default(),
            expected_text: Ok("doc\n\"{\"\"a\"\":[1,2],\"\"b\"\":\"\"x y\"\"}\"\n5\n"),
        },
        CsvRenderTestCase {
            description: "a JSON column holding something that is not JSON fails",
            columns: vec![json_column("doc", vec![Some("{oops")])],
            csv: CsvOutput::default(),
            expected_text: Err("doc: \"{oops\" is not JSON"),
        },
    ];
    for test_case in test_cases {
        let text = csv_text(&test_case);
        assert_eq!(
            text.as_deref().ok(),
            test_case.expected_text.ok(),
            "{}",
            test_case.description
        );
        assert!(
            text.err()
                .unwrap_or_default()
                .contains(test_case.expected_text.err().unwrap_or_default()),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_compression_when_writing_csv_then_the_file_decompresses_to_the_plain_text()
-> Result<(), String> {
    let test_cases = [
        CsvCompressionTestCase {
            description: "uncompressed",
            compression: OutputCompression::None,
            rows: 20_000,
            expected_magic: b"line\n",
        },
        CsvCompressionTestCase {
            description: "gzip",
            compression: OutputCompression::Gzip,
            rows: 20_000,
            expected_magic: GZIP_MAGIC,
        },
        CsvCompressionTestCase {
            description: "zstd within one frame",
            compression: OutputCompression::Zstd,
            rows: 20_000,
            expected_magic: ZSTD_MAGIC,
        },
        CsvCompressionTestCase {
            description: "zstd over several frames keeps the text after the last full frame",
            compression: OutputCompression::Zstd,
            rows: 95_000,
            expected_magic: ZSTD_MAGIC,
        },
    ];
    for test_case in test_cases {
        let lines: Vec<String> = (0..test_case.rows)
            .map(|row| format!("{row:09}{PADDING}"))
            .collect();
        let schema = Arc::new(Schema::new(vec![Field::new("line", DataType::Utf8, true)]));
        let batches: Vec<RecordBatch> = lines
            .chunks(10_000)
            .map(|chunk| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(StringArray::from(chunk.to_vec())) as ArrayRef],
                )
                .map_err(|error| error.to_string())
            })
            .collect::<Result<_, String>>()?;
        let encoding = OutputEncoding {
            format: FilesFormat::Csv,
            compression: test_case.compression,
            ..OutputEncoding::default()
        };
        let bytes = csv_file(&batches, &encoding)?;
        let expected_text = format!("line\n{}\n", lines.join("\n"));
        assert!(
            bytes.starts_with(test_case.expected_magic),
            "{}",
            test_case.description
        );
        assert_eq!(
            decompressed(test_case.compression, &bytes)?,
            expected_text.into_bytes(),
            "{}",
            test_case.description
        );
    }
    Ok(())
}
