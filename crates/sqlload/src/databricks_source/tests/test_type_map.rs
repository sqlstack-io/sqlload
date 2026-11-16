use crate::column_types::models::ColumnType;
use crate::databricks_source::_helpers::type_map::source_column;
use crate::databricks_source::tests::test_types::SourceColumnTestCase;

#[test]
fn given_databricks_column_types_when_mapping_them_then_each_gets_its_portable_type_or_a_reason() {
    let test_cases = [
        SourceColumnTestCase {
            description: "BIGINT is int",
            sql_type: "bigint",
            expected_type: Some(ColumnType::Int),
        },
        SourceColumnTestCase {
            description: "TINYINT fits a smallint",
            sql_type: "TINYINT",
            expected_type: Some(ColumnType::SmallInt),
        },
        SourceColumnTestCase {
            description: "a decimal keeps its precision and scale, spaces and all",
            sql_type: "decimal(18, 2)",
            expected_type: Some(ColumnType::Decimal {
                precision: 18,
                scale: 2,
            }),
        },
        SourceColumnTestCase {
            description: "a bare DECIMAL is Databricks' default decimal(10,0)",
            sql_type: "DECIMAL",
            expected_type: Some(ColumnType::Decimal {
                precision: 10,
                scale: 0,
            }),
        },
        SourceColumnTestCase {
            description: "TIMESTAMP is an instant, TIMESTAMP_NTZ a wall-clock time",
            sql_type: "timestamp",
            expected_type: Some(ColumnType::TimestampTz),
        },
        SourceColumnTestCase {
            description: "TIMESTAMP_NTZ is not read as TIMESTAMP",
            sql_type: "timestamp_ntz",
            expected_type: Some(ColumnType::TimestampNtz),
        },
        SourceColumnTestCase {
            description: "VARCHAR keeps its length",
            sql_type: "varchar(20)",
            expected_type: Some(ColumnType::Varchar(20)),
        },
        SourceColumnTestCase {
            description: "a struct whose fields hold parentheses and commas is json",
            sql_type: "struct<a:decimal(10,2),b:string>",
            expected_type: Some(ColumnType::Json),
        },
        SourceColumnTestCase {
            description: "an array is json",
            sql_type: "array<int>",
            expected_type: Some(ColumnType::Json),
        },
        SourceColumnTestCase {
            description: "a string with a collation is still text",
            sql_type: "string collate UTF8_LCASE",
            expected_type: Some(ColumnType::Text),
        },
        SourceColumnTestCase {
            description: "a timestamp in microseconds is a timestamp",
            sql_type: "timestamp(6)",
            expected_type: Some(ColumnType::TimestampTz),
        },
        SourceColumnTestCase {
            description: "a timestamp in nanoseconds would lose digits, so it must be pinned",
            sql_type: "timestamp_ntz(9)",
            expected_type: None,
        },
        SourceColumnTestCase {
            description: "an interval has no portable type",
            sql_type: "interval day to second",
            expected_type: None,
        },
    ];
    for test_case in test_cases {
        let column = source_column("c", test_case.sql_type, true);
        assert_eq!(
            column.column_type.ok(),
            test_case.expected_type,
            "{}",
            test_case.description
        );
    }
}
