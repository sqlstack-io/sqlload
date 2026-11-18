use crate::column_types::models::ColumnType;
use crate::duckdb_source::_helpers::type_map::source_column;
use crate::duckdb_source::tests::test_types::SourceColumnTestCase;

#[test]
fn given_duckdb_column_types_when_mapping_them_then_each_gets_its_portable_type_or_must_be_pinned()
{
    let test_cases = [
        SourceColumnTestCase {
            description: "unsigned 8-bit fits smallint",
            data_type: "UTINYINT",
            expected_type: Some(ColumnType::SmallInt),
        },
        SourceColumnTestCase {
            description: "unsigned 32-bit fits a 64-bit int",
            data_type: "UINTEGER",
            expected_type: Some(ColumnType::Int),
        },
        SourceColumnTestCase {
            description: "unsigned 64-bit does not fit int",
            data_type: "UBIGINT",
            expected_type: None,
        },
        SourceColumnTestCase {
            description: "HUGEINT does not fit int",
            data_type: "HUGEINT",
            expected_type: None,
        },
        SourceColumnTestCase {
            description: "a decimal keeps precision and scale",
            data_type: "DECIMAL(12,2)",
            expected_type: Some(ColumnType::Decimal {
                precision: 12,
                scale: 2,
            }),
        },
        SourceColumnTestCase {
            description: "a 38-digit decimal is the widest",
            data_type: "DECIMAL(38,10)",
            expected_type: Some(ColumnType::Decimal {
                precision: 38,
                scale: 10,
            }),
        },
        SourceColumnTestCase {
            description: "an enum is its label",
            data_type: "ENUM('a', 'b')",
            expected_type: Some(ColumnType::Text),
        },
        SourceColumnTestCase {
            description: "TIMESTAMPTZ is an instant",
            data_type: "TIMESTAMP WITH TIME ZONE",
            expected_type: Some(ColumnType::TimestampTz),
        },
        SourceColumnTestCase {
            description: "millisecond timestamps widen to microseconds",
            data_type: "TIMESTAMP_MS",
            expected_type: Some(ColumnType::TimestampNtz),
        },
        SourceColumnTestCase {
            description: "nanosecond timestamps would lose digits",
            data_type: "TIMESTAMP_NS",
            expected_type: None,
        },
        SourceColumnTestCase {
            description: "a list is json",
            data_type: "VARCHAR[]",
            expected_type: Some(ColumnType::Json),
        },
        SourceColumnTestCase {
            description: "a fixed-size array is json",
            data_type: "INTEGER[3]",
            expected_type: Some(ColumnType::Json),
        },
        SourceColumnTestCase {
            description: "a struct is json",
            data_type: "STRUCT(a INTEGER, b VARCHAR)",
            expected_type: Some(ColumnType::Json),
        },
        SourceColumnTestCase {
            description: "an interval has no portable type",
            data_type: "INTERVAL",
            expected_type: None,
        },
    ];
    for test_case in test_cases {
        let column = source_column("c", test_case.data_type, true);
        assert_eq!(
            column.column_type.ok(),
            test_case.expected_type,
            "{}",
            test_case.description
        );
        assert_eq!(
            column.postgres_type, test_case.data_type,
            "{}: plan shows DuckDB's type",
            test_case.description
        );
    }
}
