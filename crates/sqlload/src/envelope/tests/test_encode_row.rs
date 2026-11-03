use crate::envelope::main::encode_row::encode_row;
use crate::envelope::models::RowContext;
use crate::envelope::tests::helpers::{all_columns, record};
use crate::envelope::tests::test_types::{EncodeRowTestCase, TimeZoneTestCase, UnwrapTestCase};

const MARCH_1: i64 = 1_709_251_200_000;
const JULY_1: i64 = 1_719_792_000_000;

#[test]
fn given_a_record_when_encoding_then_each_column_holds_its_field() {
    let test_cases = [
        EncodeRowTestCase {
            description: "text columns keep JSON payloads as text",
            value_type: "VARCHAR",
            value: Some(br#"{"order_id":7}"#),
            key: Some(b"customer-1"),
            expected_row: r#"{"DATE":"2024-02-29","EXPORTED_AT":"2024-03-01T00:00:00.000Z","H":{"source":"web"},"K":"customer-1","O":42,"P":3,"T":"shop.orders","TS":1709210096000,"TT":0,"V":"{\"order_id\":7}","X":null}"#,
        },
        EncodeRowTestCase {
            description: "VARIANT columns embed valid JSON as JSON",
            value_type: "VARIANT",
            value: Some(br#"{"order_id":7}"#),
            key: None,
            expected_row: r#"{"DATE":"2024-02-29","EXPORTED_AT":"2024-03-01T00:00:00.000Z","H":{"source":"web"},"K":null,"O":42,"P":3,"T":"shop.orders","TS":1709210096000,"TT":0,"V":{"order_id":7},"X":null}"#,
        },
        EncodeRowTestCase {
            description: "VARIANT columns keep big integers and long decimals exact",
            value_type: "VARIANT",
            value: Some(br#"{"id":123456789012345678901234567890,"price":0.1234567890123456789}"#),
            key: None,
            expected_row: r#"{"DATE":"2024-02-29","EXPORTED_AT":"2024-03-01T00:00:00.000Z","H":{"source":"web"},"K":null,"O":42,"P":3,"T":"shop.orders","TS":1709210096000,"TT":0,"V":{"id":123456789012345678901234567890,"price":0.1234567890123456789},"X":null}"#,
        },
        EncodeRowTestCase {
            description: "VARIANT columns fall back to text for invalid JSON",
            value_type: "VARIANT",
            value: Some(b"not json"),
            key: None,
            expected_row: r#"{"DATE":"2024-02-29","EXPORTED_AT":"2024-03-01T00:00:00.000Z","H":{"source":"web"},"K":null,"O":42,"P":3,"T":"shop.orders","TS":1709210096000,"TT":0,"V":"not json","X":null}"#,
        },
        EncodeRowTestCase {
            description: "a tombstone has a null value",
            value_type: "VARCHAR",
            value: None,
            key: None,
            expected_row: r#"{"DATE":"2024-02-29","EXPORTED_AT":"2024-03-01T00:00:00.000Z","H":{"source":"web"},"K":null,"O":42,"P":3,"T":"shop.orders","TS":1709210096000,"TT":0,"V":null,"X":null}"#,
        },
    ];
    for test_case in &test_cases {
        let row = encode_row(
            &all_columns(test_case.value_type),
            &record(test_case.key, test_case.value),
            &RowContext {
                exported_at_ms: MARCH_1,
                time_zone: chrono_tz::Tz::UTC,
                unwrap: &std::collections::BTreeMap::new(),
            },
        );
        assert_eq!(row, test_case.expected_row, "{}", test_case.description);
    }
}

#[test]
fn given_unwrap_columns_when_encoding_then_they_hold_a_member_of_the_value_as_json_text() {
    let test_cases = [
        UnwrapTestCase {
            description: "an object member becomes compact JSON text",
            value: br#"{"value": {"a": 1}, "other": 2}"#,
            expected_value: r#""{\"a\":1}""#,
        },
        UnwrapTestCase {
            description: "a missing member is null",
            value: br#"{"other": 2}"#,
            expected_value: "null",
        },
        UnwrapTestCase {
            description: "a value that is not JSON gives null",
            value: b"plain text",
            expected_value: "null",
        },
    ];
    let unwrap = std::collections::BTreeMap::from([("V".to_owned(), "value".to_owned())]);
    for test_case in &test_cases {
        let row = encode_row(
            &all_columns("VARCHAR"),
            &record(None, Some(test_case.value)),
            &RowContext {
                exported_at_ms: MARCH_1,
                time_zone: chrono_tz::Tz::UTC,
                unwrap: &unwrap,
            },
        );
        let parsed: serde_json::Value = serde_json::from_str(&row).unwrap_or_default();
        assert_eq!(
            parsed["V"].to_string(),
            test_case.expected_value,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_time_zone_when_encoding_then_exported_at_carries_its_offset() {
    let test_cases = [
        TimeZoneTestCase {
            description: "Los Angeles in winter is eight hours behind UTC",
            zone: chrono_tz::Tz::America__Los_Angeles,
            exported_at_ms: MARCH_1,
            expected_exported_at: "2024-02-29T16:00:00.000-08:00",
        },
        TimeZoneTestCase {
            description: "Los Angeles in summer is seven hours behind UTC",
            zone: chrono_tz::Tz::America__Los_Angeles,
            exported_at_ms: JULY_1,
            expected_exported_at: "2024-06-30T17:00:00.000-07:00",
        },
        TimeZoneTestCase {
            description: "UTC keeps the Z suffix",
            zone: chrono_tz::Tz::UTC,
            exported_at_ms: MARCH_1,
            expected_exported_at: "2024-03-01T00:00:00.000Z",
        },
    ];
    for test_case in &test_cases {
        let row = encode_row(
            &all_columns("VARCHAR"),
            &record(None, None),
            &RowContext {
                exported_at_ms: test_case.exported_at_ms,
                time_zone: test_case.zone,
                unwrap: &std::collections::BTreeMap::new(),
            },
        );
        let parsed: serde_json::Value = serde_json::from_str(&row).unwrap_or_default();
        assert_eq!(
            parsed["EXPORTED_AT"], test_case.expected_exported_at,
            "{}",
            test_case.description
        );
    }
}
