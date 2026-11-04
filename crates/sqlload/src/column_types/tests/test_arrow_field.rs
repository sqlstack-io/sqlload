use crate::column_types::constants::ARROW_TYPE_METADATA_KEY;
use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::models::ColumnType;
use crate::column_types::tests::test_types::ArrowFieldTestCase;

#[test]
fn given_a_portable_type_when_building_its_arrow_field_then_the_type_and_marker_match() {
    let test_cases = [
        ArrowFieldTestCase {
            description: "int is a 64-bit integer",
            column_type: ColumnType::Int,
            expected_data_type: Some("Int64"),
            expected_metadata: None,
        },
        ArrowFieldTestCase {
            description: "decimal keeps precision and scale",
            column_type: ColumnType::Decimal {
                precision: 18,
                scale: 2,
            },
            expected_data_type: Some("Decimal128(18, 2)"),
            expected_metadata: None,
        },
        ArrowFieldTestCase {
            description: "timestamp_tz is a UTC microsecond timestamp",
            column_type: ColumnType::TimestampTz,
            expected_data_type: Some("Timestamp(µs, \"UTC\")"),
            expected_metadata: None,
        },
        ArrowFieldTestCase {
            description: "timestamp_ntz has no zone",
            column_type: ColumnType::TimestampNtz,
            expected_data_type: Some("Timestamp(µs)"),
            expected_metadata: None,
        },
        ArrowFieldTestCase {
            description: "json is text marked as json",
            column_type: ColumnType::Json,
            expected_data_type: Some("Utf8"),
            expected_metadata: Some("json"),
        },
        ArrowFieldTestCase {
            description: "a native type has no Arrow field",
            column_type: ColumnType::Native("GEOGRAPHY".to_owned()),
            expected_data_type: None,
            expected_metadata: None,
        },
    ];
    for test_case in test_cases {
        let field = arrow_field("column", &test_case.column_type);
        assert_eq!(
            field
                .as_ref()
                .map(|field| field.data_type().to_string())
                .as_deref(),
            test_case.expected_data_type,
            "{}",
            test_case.description
        );
        assert_eq!(
            field
                .as_ref()
                .and_then(|field| field.metadata().get(ARROW_TYPE_METADATA_KEY))
                .map(String::as_str),
            test_case.expected_metadata,
            "{}",
            test_case.description
        );
    }
}
