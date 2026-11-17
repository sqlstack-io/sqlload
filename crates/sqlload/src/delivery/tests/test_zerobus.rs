use crate::delivery::_helpers::zerobus::{chunk_records, zerobus_kind, zerobus_record};
use crate::delivery::tests::helpers::value_columns;
use crate::delivery::tests::test_types::{ChunkRecordsTestCase, ZerobusRecordTestCase};

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
        let record = zerobus_record(&columns, &kinds, test_case.line)
            .map_err(|error| error.to_string())?;
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
