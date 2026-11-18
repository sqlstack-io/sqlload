use crate::column_types::models::ColumnType;
use crate::duckdb_source::_helpers::type_map::source_column;

#[test]
fn given_duckdb_column_types_when_mapping_them_then_each_gets_its_portable_type_or_must_be_pinned() {
    let decimal = |precision, scale| ColumnType::Decimal { precision, scale };
    let test_cases: [(&str, &str, Option<ColumnType>); 14] = [
        ("unsigned 8-bit fits smallint", "UTINYINT", Some(ColumnType::SmallInt)),
        ("unsigned 32-bit fits a 64-bit int", "UINTEGER", Some(ColumnType::Int)),
        ("unsigned 64-bit does not fit int", "UBIGINT", None),
        ("HUGEINT does not fit int", "HUGEINT", None),
        ("a decimal keeps precision and scale", "DECIMAL(12,2)", Some(decimal(12, 2))),
        ("a 38-digit decimal is the widest", "DECIMAL(38,10)", Some(decimal(38, 10))),
        ("an enum is its label", "ENUM('a', 'b')", Some(ColumnType::Text)),
        ("TIMESTAMPTZ is an instant", "TIMESTAMP WITH TIME ZONE", Some(ColumnType::TimestampTz)),
        ("millisecond timestamps widen to microseconds", "TIMESTAMP_MS", Some(ColumnType::TimestampNtz)),
        ("nanosecond timestamps would lose digits", "TIMESTAMP_NS", None),
        ("a list is json", "VARCHAR[]", Some(ColumnType::Json)),
        ("a fixed-size array is json", "INTEGER[3]", Some(ColumnType::Json)),
        ("a struct is json", "STRUCT(a INTEGER, b VARCHAR)", Some(ColumnType::Json)),
        ("an interval has no portable type", "INTERVAL", None),
    ];
    for (description, data_type, expected) in test_cases {
        let column = source_column("c", data_type, true);
        assert_eq!(column.column_type.ok(), expected, "{description}");
        assert_eq!(column.postgres_type, data_type, "{description}: plan shows DuckDB's type");
    }
}
