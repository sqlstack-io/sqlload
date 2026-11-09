use crate::bigquery_source::_helpers::type_map::source_column;
use crate::bigquery_source::tests::test_types::SourceColumnTestCase;
use crate::column_types::models::ColumnType;

#[test]
fn given_bigquery_schema_fields_when_mapping_them_then_each_gets_its_portable_type_or_a_reason() {
    let test_cases = [
        SourceColumnTestCase {
            description: "the legacy INTEGER name is int, and REQUIRED is not nullable",
            field: r#"{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}"#,
            expected_type: Some(ColumnType::Int),
            expected_shown: "INTEGER",
            expected_nullable: false,
        },
        SourceColumnTestCase {
            description: "a parameterised NUMERIC keeps its precision and scale, given as strings",
            field: r#"{"name": "amount", "type": "NUMERIC", "precision": "12", "scale": "2"}"#,
            expected_type: Some(ColumnType::Decimal {
                precision: 12,
                scale: 2,
            }),
            expected_shown: "NUMERIC",
            expected_nullable: true,
        },
        SourceColumnTestCase {
            description: "a plain NUMERIC is decimal(38,9)",
            field: r#"{"name": "plain", "type": "NUMERIC"}"#,
            expected_type: Some(ColumnType::Decimal {
                precision: 38,
                scale: 9,
            }),
            expected_shown: "NUMERIC",
            expected_nullable: true,
        },
        SourceColumnTestCase {
            description: "TIMESTAMP is a UTC instant",
            field: r#"{"name": "at", "type": "TIMESTAMP"}"#,
            expected_type: Some(ColumnType::TimestampTz),
            expected_shown: "TIMESTAMP",
            expected_nullable: true,
        },
        SourceColumnTestCase {
            description: "a repeated column is json unless pinned",
            field: r#"{"name": "tags", "type": "STRING", "mode": "REPEATED"}"#,
            expected_type: Some(ColumnType::Json),
            expected_shown: "ARRAY<STRING>",
            expected_nullable: true,
        },
        SourceColumnTestCase {
            description: "a record is json",
            field: r#"{"name": "who", "type": "RECORD", "fields": []}"#,
            expected_type: Some(ColumnType::Json),
            expected_shown: "STRUCT",
            expected_nullable: true,
        },
        SourceColumnTestCase {
            description: "GEOGRAPHY is its WKT text",
            field: r#"{"name": "where", "type": "GEOGRAPHY"}"#,
            expected_type: Some(ColumnType::Text),
            expected_shown: "GEOGRAPHY",
            expected_nullable: true,
        },
        SourceColumnTestCase {
            description: "a plain BIGNUMERIC is wider than 38 digits, so it needs a pin",
            field: r#"{"name": "wide", "type": "BIGNUMERIC"}"#,
            expected_type: None,
            expected_shown: "BIGNUMERIC",
            expected_nullable: true,
        },
        SourceColumnTestCase {
            description: "INTERVAL has no portable type",
            field: r#"{"name": "span", "type": "INTERVAL"}"#,
            expected_type: None,
            expected_shown: "INTERVAL",
            expected_nullable: true,
        },
        SourceColumnTestCase {
            description: "a size that is not a number needs a pin, rather than being dropped",
            field: r#"{"name": "label", "type": "STRING", "maxLength": "lots"}"#,
            expected_type: None,
            expected_shown: "STRING",
            expected_nullable: true,
        },
    ];
    for test_case in test_cases {
        let field: serde_json::Value =
            serde_json::from_str(test_case.field).expect("a schema field");
        let column = source_column(&field);
        assert_eq!(
            (
                column.column_type.ok(),
                column.postgres_type.as_str(),
                column.nullable
            ),
            (
                test_case.expected_type,
                test_case.expected_shown,
                test_case.expected_nullable
            ),
            "{}",
            test_case.description
        );
    }
}
