use arrow_array::RecordBatch;
use arrow_cast::display::array_value_to_string;
use prost::Message;

use crate::delivery::_helpers::bigquery::{append_chunks, load_job_id};
use crate::delivery::tests::helpers::value_columns;
use crate::delivery::tests::test_types::{
    AppendChunksTestCase, BigQueryRowsTestCase, LoadJobIdTestCase,
};
use crate::file_format::main::bigquery_rows::bigquery_rows;

#[test]
fn given_encoded_rows_when_typing_them_for_bigquery_then_each_column_takes_its_type_or_names_the_value()
 {
    let test_cases = [
        BigQueryRowsTestCase {
            description: "an INT64 column reads an unwrapped member's text",
            columns: &[("id", "INT64")],
            line: r#"{"id":"42"}"#,
            expected_value: Ok("Int64 42"),
        },
        BigQueryRowsTestCase {
            description: "the Kafka timestamp's milliseconds fill a TIMESTAMP column",
            columns: &[("sent_at", "TIMESTAMP")],
            line: r#"{"sent_at":1794906000000}"#,
            expected_value: Ok("Timestamp(ms, \"UTC\") 2026-11-17T09:00:00Z"),
        },
        BigQueryRowsTestCase {
            description: "a date and time without a zone is read as UTC",
            columns: &[("sent_at", "TIMESTAMP")],
            line: r#"{"sent_at":"2026-11-17 09:00:00"}"#,
            expected_value: Ok("Timestamp(ms, \"UTC\") 2026-11-17T09:00:00Z"),
        },
        BigQueryRowsTestCase {
            description: "a DATE column fed a timestamp takes its UTC date",
            columns: &[("day", "DATE")],
            line: r#"{"day":"2026-11-17T23:30:00-01:00"}"#,
            expected_value: Ok("Date32 2026-11-18"),
        },
        BigQueryRowsTestCase {
            description: "a value a TIMESTAMP column cannot hold is an error naming it, never a NULL",
            columns: &[("sent_at", "TIMESTAMP")],
            line: r#"{"sent_at":"soon"}"#,
            expected_value: Err("column sent_at: \"soon\" is not a timestamp"),
        },
    ];
    for test_case in &test_cases {
        let typed = bigquery_rows(&value_columns(test_case.columns), &[test_case.line]).and_then(
            |rows: RecordBatch| {
                let column = rows.column(0);
                array_value_to_string(column, 0)
                    .map(|value| format!("{} {value}", column.data_type()))
                    .map_err(|error| error.to_string())
            },
        );
        assert_eq!(
            typed,
            test_case
                .expected_value
                .map(str::to_owned)
                .map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_stream_and_batch_when_naming_its_load_job_then_the_name_is_stable_and_valid() {
    let test_cases = [
        LoadJobIdTestCase {
            description: "the same batch and attempt give the same job, so a retry finds it",
            stream: "shop__orders",
            attempt: 0,
            expected_id: "sqlload_shop__orders_0a1b_0",
        },
        LoadJobIdTestCase {
            description: "a later attempt after a failed job is another job",
            stream: "shop__orders",
            attempt: 2,
            expected_id: "sqlload_shop__orders_0a1b_2",
        },
        LoadJobIdTestCase {
            description: "characters a job id cannot hold become underscores",
            stream: "shop.orders$v2",
            attempt: 0,
            expected_id: "sqlload_shop_orders_v2_0a1b_0",
        },
    ];
    for test_case in &test_cases {
        assert_eq!(
            load_job_id(test_case.stream, "0a1b", test_case.attempt),
            test_case.expected_id,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_rows_when_cutting_appends_then_each_request_fits_the_limit_without_an_offset()
-> Result<(), String> {
    let stream = "projects/p/datasets/d/tables/t/streams/_default";
    let test_cases = [
        AppendChunksTestCase {
            description: "many rows split into requests under the limit",
            rows: 2_000,
            limit: 16_384,
            expected_split: true,
            expected_faults: (0, 0, 0),
        },
        AppendChunksTestCase {
            description: "a few rows go in one request",
            rows: 3,
            limit: 1_000_000,
            expected_split: false,
            expected_faults: (0, 0, 0),
        },
    ];
    let columns = value_columns(&[("value", "STRING")]);
    for test_case in &test_cases {
        let lines: Vec<String> = (0..test_case.rows)
            .map(|index| format!(r#"{{"value":"row {index:05} {}"}}"#, "x".repeat(index % 50)))
            .collect();
        let lines: Vec<&str> = lines.iter().map(String::as_str).collect();
        let requests = append_chunks(&bigquery_rows(&columns, &lines)?, test_case.limit, stream)
            .map_err(|error| error.to_string())?;
        let faults = (
            requests
                .iter()
                .filter(|request| request.encoded_len() > test_case.limit)
                .count(),
            requests
                .iter()
                .filter(|request| request.offset.is_some())
                .count(),
            requests
                .iter()
                .filter(|request| request.write_stream != stream)
                .count(),
        );
        assert_eq!(
            (requests.len() > 1, faults),
            (test_case.expected_split, test_case.expected_faults),
            "{}",
            test_case.description
        );
    }
    Ok(())
}
