use databricks_zerobus_ingest_sdk::{GrpcCode, GrpcStatus, ZerobusError};

use crate::delivery::_helpers::zerobus::{chunk_records, open_error, zerobus_kind, zerobus_record};
use crate::delivery::errors::DeliveryError;
use crate::delivery::tests::helpers::value_columns;
use crate::delivery::tests::test_types::{
    ChunkRecordsTestCase, OpenErrorTestCase, ZerobusRecordTestCase,
};

#[test]
fn given_an_encoded_row_when_streaming_to_zerobus_then_typed_columns_take_its_json_forms()
-> Result<(), String> {
    let test_cases = [
        ZerobusRecordTestCase {
            description: "a zoned timestamp becomes UTC microseconds",
            columns: &[("exported_at", "TIMESTAMP")],
            line: r#"{"exported_at":"2026-11-17T09:00:00.123-08:00"}"#,
            expected_record: r#"{"exported_at":1794934800123000}"#,
        },
        ZerobusRecordTestCase {
            description: "a date and time without a zone, from user data, is read as UTC",
            columns: &[("sent_at", "TIMESTAMP")],
            line: r#"{"sent_at":"2026-11-17 09:00:00"}"#,
            expected_record: r#"{"sent_at":1794906000000000}"#,
        },
        ZerobusRecordTestCase {
            description: "a date column fed a timestamp takes its UTC date",
            columns: &[("day", "DATE")],
            line: r#"{"day":"1970-01-03T23:30:00-01:00"}"#,
            expected_record: r#"{"day":3}"#,
        },
        ZerobusRecordTestCase {
            description: "an unwrapped member arrives as quoted JSON text and is still read",
            columns: &[("sent_at", "TIMESTAMP"), ("day", "DATE")],
            line: r#"{"sent_at":"\"2026-11-17T09:00:00Z\"","day":"\"1970-01-03\""}"#,
            expected_record: r#"{"sent_at":1794906000000000,"day":2}"#,
        },
        ZerobusRecordTestCase {
            description: "a date becomes days since the epoch",
            columns: &[("date", "DATE")],
            line: r#"{"date":"1970-01-03"}"#,
            expected_record: r#"{"date":2}"#,
        },
        ZerobusRecordTestCase {
            description: "a VARIANT value is sent as its JSON text",
            columns: &[("payload", "VARIANT")],
            line: r#"{"payload":{"a":[1,"x"]}}"#,
            expected_record: r#"{"payload":"{\"a\":[1,\"x\"]}"}"#,
        },
        ZerobusRecordTestCase {
            description: "strings and nulls pass through",
            columns: &[("value", "STRING"), ("missing", "TIMESTAMP")],
            line: r#"{"value":"{\"a\":1}","missing":null}"#,
            expected_record: r#"{"value":"{\"a\":1}","missing":null}"#,
        },
    ];
    for test_case in &test_cases {
        let columns = value_columns(test_case.columns);
        let kinds: Vec<_> = columns.iter().map(zerobus_kind).collect();
        let record =
            zerobus_record(&columns, &kinds, test_case.line).map_err(|error| error.to_string())?;
        let parsed: serde_json::Value =
            serde_json::from_str(&record).map_err(|error| error.to_string())?;
        assert_eq!(
            Ok(parsed),
            serde_json::from_str::<serde_json::Value>(test_case.expected_record)
                .map_err(|error| error.to_string()),
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_records_when_chunking_for_zerobus_then_requests_stay_under_the_limit_in_order() {
    let test_cases = [
        ChunkRecordsTestCase {
            description: "records fill a request up to the limit, and an oversized one goes alone",
            records: &[(4, "a"), (4, "b"), (3, "c"), (9, "d"), (1, "e")],
            limit: 8,
            expected_requests: &[&["a", "b"], &["c"], &["d"], &["e"]],
        },
        ChunkRecordsTestCase {
            description: "small records share one request",
            records: &[(1, "a"), (2, "b"), (3, "c")],
            limit: 8,
            expected_requests: &[&["a", "b", "c"]],
        },
    ];
    for test_case in &test_cases {
        let records: Vec<(u64, String)> = test_case
            .records
            .iter()
            .map(|(bytes, text)| (*bytes, (*text).to_owned()))
            .collect();
        assert_eq!(
            chunk_records(records, test_case.limit),
            test_case.expected_requests,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_stream_that_would_not_open_when_classifying_it_then_only_transient_failures_and_authorisation_refusals_retry()
 {
    let hint = "; if this lasts, the service principal sp-1 needs USE CATALOG, USE SCHEMA, and MODIFY and SELECT on the table, granted explicitly";
    let test_cases = [
        OpenErrorTestCase {
            description: "a refused token request retries, naming the grants",
            error: ZerobusError::InvalidUCTokenError("Client error (401)".to_owned()),
            expected_error: DeliveryError::Retry(format!(
                "Zerobus Ingest open stream for c.s.t was refused: Specified UC token is in invalid format: Client error (401).{hint}"
            )),
        },
        OpenErrorTestCase {
            description: "a stream refused for permissions retries, naming the grants",
            error: ZerobusError::CreateStreamError(GrpcStatus::new(
                GrpcCode::PermissionDenied,
                "no MODIFY",
            )),
            expected_error: DeliveryError::Retry(format!(
                "Zerobus Ingest open stream for c.s.t was refused: {}{hint}",
                ZerobusError::CreateStreamError(GrpcStatus::new(
                    GrpcCode::PermissionDenied,
                    "no MODIFY"
                ))
            )),
        },
        OpenErrorTestCase {
            description: "a token request refused as a bad client, such as a wrong secret, stops the exporter",
            error: ZerobusError::InvalidUCTokenError("Client error (401): invalid_client".to_owned()),
            expected_error: DeliveryError::Fatal(
                "Zerobus Ingest open stream failed: Specified UC token is in invalid format: Client error (401): invalid_client."
                    .to_owned(),
            ),
        },
        OpenErrorTestCase {
            description: "a timeout retries as the SDK classes it",
            error: ZerobusError::ConnectionTimeout("10s".to_owned()),
            expected_error: DeliveryError::Retry(
                "Zerobus Ingest open stream failed: Connection timeout: 10s".to_owned(),
            ),
        },
        OpenErrorTestCase {
            description: "a missing table stops the exporter",
            error: ZerobusError::CreateStreamError(GrpcStatus::new(GrpcCode::NotFound, "no table")),
            expected_error: DeliveryError::Fatal(format!(
                "Zerobus Ingest open stream failed: {}",
                ZerobusError::CreateStreamError(GrpcStatus::new(GrpcCode::NotFound, "no table"))
            )),
        },
        OpenErrorTestCase {
            description: "an invalid table name stops the exporter",
            error: ZerobusError::InvalidTableName("c.s".to_owned()),
            expected_error: DeliveryError::Fatal(
                "Zerobus Ingest open stream failed: Specified UC table name is invalid: c.s."
                    .to_owned(),
            ),
        },
    ];
    for test_case in &test_cases {
        assert_eq!(
            open_error("c.s.t", "sp-1", &test_case.error),
            test_case.expected_error,
            "{}",
            test_case.description
        );
    }
}
