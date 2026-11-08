use std::sync::Arc;

use arrow_array::types::Int64Type;
use arrow_array::{
    Decimal128Array, Int32Array, Int64Array, LargeStringArray, ListArray, TimestampMillisecondArray,
};
use prost::Message;

use crate::adapters::bigquery::_helpers::write::arrow_rows::{append_requests, write_api_batch};
use crate::adapters::bigquery::tests::helpers::{
    batch_of, decimal_lists, decoded_ids, desired, noted_ids, shown,
};
use crate::adapters::bigquery::tests::test_types::{AppendRequestsTestCase, WriteApiBatchTestCase};
use crate::column_types::models::ColumnType;

#[test]
fn given_planned_columns_when_preparing_rows_for_the_write_api_then_each_takes_the_arrow_type_bigquery_reads()
-> Result<(), String> {
    let test_cases = [
        WriteApiBatchTestCase {
            builds: false,
            description: "integers widen to INT64, and a decimal of only fraction digits gains the digit its NUMERIC has",
            columns: vec![
                desired("id", ColumnType::Int),
                desired(
                    "share",
                    ColumnType::Decimal {
                        precision: 5,
                        scale: 5,
                    },
                ),
            ],
            batch: batch_of(vec![
                ("id", Arc::new(Int32Array::from(vec![Some(1), None]))),
                (
                    "share",
                    Arc::new(
                        Decimal128Array::from(vec![12345, 5])
                            .with_precision_and_scale(5, 5)
                            .map_err(|error| error.to_string())?,
                    ),
                ),
            ])?,
            expected_batch: Ok("id: Int64 = 1|null; share: Decimal128(6, 5) = 0.12345|0.00005"),
        },
        WriteApiBatchTestCase {
            builds: false,
            description: "a decimal too wide for NUMERIC goes as Decimal256, which is all BIGNUMERIC reads",
            columns: vec![desired(
                "amount",
                ColumnType::Decimal {
                    precision: 38,
                    scale: 10,
                },
            )],
            batch: batch_of(vec![(
                "amount",
                Arc::new(
                    Decimal128Array::from(vec![15_000_000_000])
                        .with_precision_and_scale(38, 10)
                        .map_err(|error| error.to_string())?,
                ),
            )])?,
            expected_batch: Ok("amount: Decimal256(38, 10) = 1.5000000000"),
        },
        WriteApiBatchTestCase {
            builds: false,
            description: "a timestamp with an offset keeps its instant, in UTC microseconds; JSON goes as its text",
            columns: vec![
                desired("at", ColumnType::TimestampTz),
                desired("payload", ColumnType::Json),
            ],
            batch: batch_of(vec![
                (
                    "at",
                    Arc::new(
                        TimestampMillisecondArray::from(vec![3_600_000]).with_timezone("+01:00"),
                    ),
                ),
                (
                    "payload",
                    Arc::new(LargeStringArray::from(vec!["{\"a\":1}"])),
                ),
            ])?,
            expected_batch: Ok(
                "at: Timestamp(µs, \"UTC\") = 1970-01-01T01:00:00Z; payload: Utf8 = {\"a\":1}",
            ),
        },
        WriteApiBatchTestCase {
            builds: false,
            description: "a value too large for its column fails the batch rather than loading as NULL",
            columns: vec![desired(
                "amount",
                ColumnType::Decimal {
                    precision: 4,
                    scale: 2,
                },
            )],
            batch: batch_of(vec![("amount", Arc::new(Int64Array::from(vec![12, 123])))])?,
            expected_batch: Err("column amount cannot be sent to BigQuery as Decimal128(4, 2)"),
        },
        WriteApiBatchTestCase {
            builds: false,
            description: "a nested value is refused by column",
            columns: vec![desired(
                "tags",
                ColumnType::Native("ARRAY<INT64>".to_owned()),
            )],
            batch: batch_of(vec![(
                "tags",
                Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                    Some(vec![Some(1)]),
                ])),
            )])?,
            expected_batch: Err("column tags (List(Int64)) cannot load into BigQuery yet"),
        },
        WriteApiBatchTestCase {
            builds: false,
            description: "an array of decimals goes at NUMERIC's own scale, which is how the Write API reads an element whatever its Arrow scale; a NULL array goes as an empty one",
            columns: vec![desired(
                "amounts",
                ColumnType::Array(Box::new(ColumnType::Decimal {
                    precision: 10,
                    scale: 2,
                })),
            )],
            batch: batch_of(vec![(
                "amounts",
                Arc::new(decimal_lists(vec![
                    Some(vec![Some(150), Some(-225)]),
                    None,
                ])?),
            )])?,
            expected_batch: Ok(
                "amounts: List(non-null Decimal128(38, 9)) = [1.500000000, -2.250000000]|[]",
            ),
        },
        WriteApiBatchTestCase {
            builds: false,
            description: "a NULL element fails the batch, as a BigQuery array cannot hold one",
            columns: vec![desired("ids", ColumnType::Array(Box::new(ColumnType::Int)))],
            batch: batch_of(vec![(
                "ids",
                Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                    Some(vec![Some(1)]),
                    Some(vec![Some(2), None]),
                ])),
            )])?,
            expected_batch: Err("column ids"),
        },
        WriteApiBatchTestCase {
            builds: false,
            description: "a nested value is refused even for a text column, which a cast would turn into display text",
            columns: vec![desired("tags", ColumnType::Text)],
            batch: batch_of(vec![(
                "tags",
                Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                    Some(vec![Some(1)]),
                ])),
            )])?,
            expected_batch: Err("column tags (List(Int64)) cannot load into BigQuery yet"),
        },
        WriteApiBatchTestCase {
            builds: true,
            description: "a field the columns leave out is not sent to a build table; large text in a native column goes as Utf8",
            columns: vec![desired("code", ColumnType::Native("STRING".to_owned()))],
            batch: batch_of(vec![
                ("code", Arc::new(LargeStringArray::from(vec!["a"]))),
                ("ignored", Arc::new(Int64Array::from(vec![7]))),
            ])?,
            expected_batch: Ok("code: Utf8 = a"),
        },
        WriteApiBatchTestCase {
            builds: false,
            description: "a field the columns leave out fails a staging table's batch rather than going unloaded",
            columns: vec![desired("code", ColumnType::Text)],
            batch: batch_of(vec![
                ("code", Arc::new(LargeStringArray::from(vec!["a"]))),
                ("ignored", Arc::new(Int64Array::from(vec![7]))),
            ])?,
            expected_batch: Err("column ignored is not among the table's planned columns"),
        },
    ];
    for test_case in test_cases {
        let prepared = write_api_batch(&test_case.batch, (&test_case.columns, test_case.builds))
            .map_err(|error| error.split(": ").next().unwrap_or_default().to_owned())
            .and_then(|batch| shown(&batch).map_err(|error| format!("unshown: {error}")));
        assert_eq!(
            prepared,
            test_case
                .expected_batch
                .map(str::to_owned)
                .map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_a_batch_when_cutting_it_into_append_requests_then_each_fits_and_every_row_decodes_back_once()
-> Result<(), String> {
    let test_cases = [
        AppendRequestsTestCase {
            description: "a batch larger than one request is cut into requests under the limit, in order",
            rows: 2000,
            note_bytes: 20,
            sliced_rows: 2000,
            max_bytes: 8 * 1024,
            expected_within_limit: true,
            expected_split: true,
            expected_ids: "0..=1999 (2000)".to_owned(),
        },
        AppendRequestsTestCase {
            description: "a batch under the limit is one request",
            rows: 10,
            note_bytes: 20,
            sliced_rows: 10,
            max_bytes: 8 * 1024,
            expected_within_limit: true,
            expected_split: false,
            expected_ids: "0..=9 (10)".to_owned(),
        },
        AppendRequestsTestCase {
            description: "a row larger than the limit goes alone, for BigQuery to refuse",
            rows: 1,
            note_bytes: 10_000,
            sliced_rows: 1,
            max_bytes: 8 * 1024,
            expected_within_limit: false,
            expected_split: false,
            expected_ids: "0..=0 (1)".to_owned(),
        },
        AppendRequestsTestCase {
            description: "a small slice of a large batch is one request, not cut by the size of the buffers it shares",
            rows: 2000,
            note_bytes: 20,
            sliced_rows: 100,
            max_bytes: 8 * 1024,
            expected_within_limit: true,
            expected_split: false,
            expected_ids: "0..=99 (100)".to_owned(),
        },
        AppendRequestsTestCase {
            description: "an empty batch is no request",
            rows: 0,
            note_bytes: 20,
            sliced_rows: 0,
            max_bytes: 8 * 1024,
            expected_within_limit: true,
            expected_split: false,
            expected_ids: "-1..=-1 (0)".to_owned(),
        },
    ];
    for test_case in test_cases {
        let requests = append_requests(
            &noted_ids(test_case.rows, test_case.note_bytes)?.slice(0, test_case.sliced_rows),
            test_case.max_bytes,
        )?;
        assert_eq!(
            (
                requests
                    .iter()
                    .all(|(request, _)| request.encoded_len() <= test_case.max_bytes),
                requests.len() > 1,
                decoded_ids(&requests)?,
                requests.iter().map(|(_, rows)| *rows).sum::<usize>(),
            ),
            (
                test_case.expected_within_limit,
                test_case.expected_split,
                test_case.expected_ids,
                test_case.sliced_rows,
            ),
            "{}",
            test_case.description
        );
    }
    Ok(())
}
