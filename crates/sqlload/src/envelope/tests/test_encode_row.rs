use crate::envelope::main::encode_row::encode_row;
use crate::envelope::tests::helpers::{all_columns, record};
use crate::envelope::tests::test_types::EncodeRowTestCase;

const MARCH_1: i64 = 1_709_251_200_000;

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
            MARCH_1,
        );
        assert_eq!(row, test_case.expected_row, "{}", test_case.description);
    }
}
