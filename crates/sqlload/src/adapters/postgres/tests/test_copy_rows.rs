use std::sync::Arc;

use arrow_array::types::Decimal128Type;
use arrow_array::{
    BinaryArray, BooleanArray, Date32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    PrimitiveArray, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};

use crate::adapters::postgres::_helpers::copy_rows::numeric_bytes;
use crate::adapters::postgres::tests::helpers::{numeric_wire, round_trip};
use crate::adapters::postgres::tests::test_types::{CopyRoundTripTestCase, NumericTestCase};
use crate::column_types::models::ColumnType;
use crate::postgres_source::models::WireType;

#[test]
fn given_batches_when_encoded_as_binary_copy_then_the_source_reader_reads_the_same_values_or_the_batch_fails()
-> Result<(), String> {
    let decimal = PrimitiveArray::<Decimal128Type>::from(vec![Some(1234), Some(-5), Some(0)])
        .with_precision_and_scale(12, 2)
        .map_err(|error| error.to_string())?;
    let test_cases = [
        CopyRoundTripTestCase {
            description: "every type, with NULLs, negatives, values before both epochs and an Int32 cast to bigint, cut at row boundaries",
            columns: vec![
                (
                    "id",
                    ColumnType::Int,
                    WireType::Int8,
                    Arc::new(Int32Array::from(vec![Some(1), None, Some(-3)])),
                ),
                (
                    "small",
                    ColumnType::SmallInt,
                    WireType::Int2,
                    Arc::new(Int16Array::from(vec![Some(-32768), Some(0), None])),
                ),
                (
                    "total",
                    ColumnType::Decimal {
                        precision: 12,
                        scale: 2,
                    },
                    WireType::Numeric,
                    Arc::new(decimal),
                ),
                (
                    "ratio",
                    ColumnType::Float,
                    WireType::Float8,
                    Arc::new(Float64Array::from(vec![Some(1.5), Some(-2.25), None])),
                ),
                (
                    "name",
                    ColumnType::Text,
                    WireType::Text,
                    Arc::new(StringArray::from(vec![Some("é"), Some(""), None])),
                ),
                (
                    "uid",
                    ColumnType::Uuid,
                    WireType::Text,
                    Arc::new(StringArray::from(vec![
                        Some("11111111-1111-1111-1111-111111111111"),
                        None,
                        None,
                    ])),
                ),
                (
                    "day",
                    ColumnType::Date,
                    WireType::Date,
                    Arc::new(Date32Array::from(vec![Some(-1), Some(10_957), None])),
                ),
                (
                    "at_time",
                    ColumnType::Time,
                    WireType::Time,
                    Arc::new(Time64MicrosecondArray::from(vec![
                        Some(0),
                        Some(86_399_999_999),
                        None,
                    ])),
                ),
                (
                    "at",
                    ColumnType::TimestampTz,
                    WireType::Timestamp,
                    Arc::new(
                        TimestampMicrosecondArray::from(vec![
                            Some(-1),
                            Some(946_684_800_000_001),
                            None,
                        ])
                        .with_timezone("UTC"),
                    ),
                ),
                (
                    "local",
                    ColumnType::TimestampNtz,
                    WireType::Timestamp,
                    Arc::new(TimestampMicrosecondArray::from(vec![
                        None,
                        Some(0),
                        Some(1_000_000),
                    ])),
                ),
                (
                    "raw",
                    ColumnType::Binary,
                    WireType::Bytea,
                    Arc::new(BinaryArray::from(vec![
                        Some(&b"\x00\x01"[..]),
                        Some(&b""[..]),
                        None,
                    ])),
                ),
                (
                    "ok",
                    ColumnType::Bool,
                    WireType::Bool,
                    Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
                ),
            ],
            chunk_bytes: 16,
            expected_result: Ok([
                "split: true; id = 1|null|-3",
                "small = -32768|0|null",
                "total = 12.34|-0.05|0.00",
                "ratio = 1.5|-2.25|null",
                "name = é||null",
                "uid = 11111111-1111-1111-1111-111111111111|null|null",
                "day = 1969-12-31|2000-01-01|null",
                "at_time = 00:00:00|23:59:59.999999|null",
                "at = 1969-12-31T23:59:59.999999Z|2000-01-01T00:00:00.000001Z|null",
                "local = null|1970-01-01T00:00:00|1970-01-01T00:00:01",
                "raw = 0001||null",
                "ok = true|false|null",
            ]
            .join("; ")),
        },
        CopyRoundTripTestCase {
            description: "a value too large for its column fails the batch, naming the column, rather than becoming NULL",
            columns: vec![(
                "small",
                ColumnType::SmallInt,
                WireType::Int2,
                Arc::new(Int64Array::from(vec![1, 40_000])),
            )],
            chunk_bytes: 1024,
            expected_result: Err("column small (Int64) cannot be sent to Postgres as smallint"),
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            round_trip(&test_case.columns, test_case.chunk_bytes)
                .as_deref()
                .map_err(String::as_str),
            test_case.expected_result.as_deref().map_err(|error| *error),
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_decimals_when_encoding_numeric_then_digits_weight_and_sign_are_postgres_own() {
    let test_cases = [
        NumericTestCase {
            description: "digits on both sides of the point",
            value: 1234,
            scale: 2,
            expected_digits: &[12, 3400],
            expected_weight: 0,
            expected_sign: 0x0000,
        },
        NumericTestCase {
            description: "a negative value below one starts after the point",
            value: -5,
            scale: 2,
            expected_digits: &[500],
            expected_weight: -1,
            expected_sign: 0x4000,
        },
        NumericTestCase {
            description: "zero has no digits and is never negative",
            value: 0,
            scale: 2,
            expected_digits: &[],
            expected_weight: 0,
            expected_sign: 0x0000,
        },
        NumericTestCase {
            description: "trailing zero groups are dropped",
            value: 100_000_000,
            scale: 0,
            expected_digits: &[1],
            expected_weight: 2,
            expected_sign: 0x0000,
        },
        NumericTestCase {
            description: "leading zero groups after the point move the weight down",
            value: 5,
            scale: 5,
            expected_digits: &[5000],
            expected_weight: -2,
            expected_sign: 0x0000,
        },
        NumericTestCase {
            description: "a long value splits into groups of four from the point",
            value: 12_345_678_999,
            scale: 2,
            expected_digits: &[1, 2345, 6789, 9900],
            expected_weight: 2,
            expected_sign: 0x0000,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            numeric_bytes(test_case.value, test_case.scale),
            numeric_wire(
                test_case.expected_digits,
                test_case.expected_weight,
                test_case.expected_sign,
                test_case.scale
            ),
            "{}",
            test_case.description
        );
    }
}
