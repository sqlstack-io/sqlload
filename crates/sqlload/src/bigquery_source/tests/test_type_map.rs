use serde_json::json;

use crate::bigquery_source::_helpers::type_map::source_column;
use crate::column_types::models::ColumnType;

#[test]
fn given_bigquery_schema_fields_when_mapping_them_then_each_gets_its_portable_type_or_a_reason() {
    let test_cases = [
        (
            json!({"name": "id", "type": "INTEGER", "mode": "REQUIRED"}),
            Ok(ColumnType::Int),
            "INTEGER",
        ),
        (
            json!({"name": "amount", "type": "NUMERIC", "precision": "12", "scale": "2"}),
            Ok(ColumnType::Decimal {
                precision: 12,
                scale: 2,
            }),
            "NUMERIC",
        ),
        (
            json!({"name": "plain", "type": "NUMERIC"}),
            Ok(ColumnType::Decimal {
                precision: 38,
                scale: 9,
            }),
            "NUMERIC",
        ),
        (
            json!({"name": "at", "type": "TIMESTAMP"}),
            Ok(ColumnType::TimestampTz),
            "TIMESTAMP",
        ),
        (
            json!({"name": "tags", "type": "STRING", "mode": "REPEATED"}),
            Ok(ColumnType::Json),
            "ARRAY<STRING>",
        ),
        (
            json!({"name": "who", "type": "RECORD", "fields": []}),
            Ok(ColumnType::Json),
            "STRUCT",
        ),
        (
            json!({"name": "where", "type": "GEOGRAPHY"}),
            Ok(ColumnType::Text),
            "GEOGRAPHY",
        ),
        (
            json!({"name": "wide", "type": "BIGNUMERIC"}),
            Err(()),
            "BIGNUMERIC",
        ),
        (
            json!({"name": "span", "type": "INTERVAL"}),
            Err(()),
            "INTERVAL",
        ),
    ];
    for (field, expected, shown) in test_cases {
        let column = source_column(&field);
        assert_eq!(column.column_type.map_err(|_| ()), expected, "{field}");
        assert_eq!(column.postgres_type, shown, "{field}");
    }
    assert!(!source_column(&json!({"name": "id", "type": "INT64", "mode": "REQUIRED"})).nullable);
}
