use crate::column_types::main::parse_type::parse_type;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::column_types::tests::test_types::{ParseTypeErrorTestCase, ParseTypeTestCase};

const SNOWFLAKE: &[Warehouse] = &[Warehouse::Snowflake];
const DUCKDB: &[Warehouse] = &[Warehouse::DuckDb];
const MIXED: &[Warehouse] = &[Warehouse::Snowflake, Warehouse::DuckDb];

#[test]
fn given_a_valid_type_when_parsing_then_it_maps_onto_the_portable_vocabulary() {
    let test_cases = [
        ParseTypeTestCase {
            description: "a portable name is kept as written",
            text: "int",
            destinations: MIXED,
            expected_type: ColumnType::Int,
            expected_written_as: None,
            expected_warnings: 0,
        },
        ParseTypeTestCase {
            description: "a decimal with spaces and capitals is normalised but not reported",
            text: " Decimal( 18, 2 ) ",
            destinations: MIXED,
            expected_type: ColumnType::Decimal {
                precision: 18,
                scale: 2,
            },
            expected_written_as: None,
            expected_warnings: 0,
        },
        ParseTypeTestCase {
            description: "an array of a scalar type",
            text: "array(int)",
            destinations: MIXED,
            expected_type: ColumnType::Array(Box::new(ColumnType::Int)),
            expected_written_as: None,
            expected_warnings: 0,
        },
        ParseTypeTestCase {
            description: "an array's element takes arguments of its own and any case, reported as written",
            text: "ARRAY( Numeric(10, 2) )",
            destinations: MIXED,
            expected_type: ColumnType::Array(Box::new(ColumnType::Decimal {
                precision: 10,
                scale: 2,
            })),
            expected_written_as: Some("ARRAY( Numeric(10, 2) )"),
            expected_warnings: 0,
        },
        ParseTypeTestCase {
            description: "a standard SQL name is accepted for any destination and reported",
            text: "BIGINT",
            destinations: MIXED,
            expected_type: ColumnType::Int,
            expected_written_as: Some("BIGINT"),
            expected_warnings: 0,
        },
        ParseTypeTestCase {
            description: "string is an alias for text",
            text: "string",
            destinations: MIXED,
            expected_type: ColumnType::Text,
            expected_written_as: Some("string"),
            expected_warnings: 0,
        },
        ParseTypeTestCase {
            description: "varchar without a length is text",
            text: "VARCHAR",
            destinations: MIXED,
            expected_type: ColumnType::Text,
            expected_written_as: Some("VARCHAR"),
            expected_warnings: 0,
        },
        ParseTypeTestCase {
            description: "a standard timestamp with a time zone is timestamp_tz",
            text: "timestamp with time zone",
            destinations: MIXED,
            expected_type: ColumnType::TimestampTz,
            expected_written_as: Some("timestamp with time zone"),
            expected_warnings: 0,
        },
        ParseTypeTestCase {
            description: "a numeric with only a precision has scale zero",
            text: "NUMERIC(10)",
            destinations: MIXED,
            expected_type: ColumnType::Decimal {
                precision: 10,
                scale: 0,
            },
            expected_written_as: Some("NUMERIC(10)"),
            expected_warnings: 0,
        },
        ParseTypeTestCase {
            description: "a Snowflake name is accepted when every destination is Snowflake",
            text: "NUMBER",
            destinations: SNOWFLAKE,
            expected_type: ColumnType::Decimal {
                precision: 38,
                scale: 0,
            },
            expected_written_as: Some("NUMBER"),
            expected_warnings: 0,
        },
        ParseTypeTestCase {
            description: "VARIANT on Snowflake is json",
            text: "VARIANT",
            destinations: SNOWFLAKE,
            expected_type: ColumnType::Json,
            expected_written_as: Some("VARIANT"),
            expected_warnings: 0,
        },
        ParseTypeTestCase {
            description: "a name DuckDB shares with Postgres is accepted when every destination is DuckDB",
            text: "timestamptz",
            destinations: DUCKDB,
            expected_type: ColumnType::TimestampTz,
            expected_written_as: Some("timestamptz"),
            expected_warnings: 0,
        },
        ParseTypeTestCase {
            description: "a fractional-seconds precision is accepted and fixed at microseconds",
            text: "timestamp_ntz(9)",
            destinations: MIXED,
            expected_type: ColumnType::TimestampNtz,
            expected_written_as: Some("timestamp_ntz(9)"),
            expected_warnings: 0,
        },
        ParseTypeTestCase {
            description: "a native type keeps its case and passes through for one warehouse",
            text: "native(\"GEOGRAPHY\")",
            destinations: SNOWFLAKE,
            expected_type: ColumnType::Native("GEOGRAPHY".to_owned()),
            expected_written_as: None,
            expected_warnings: 0,
        },
        ParseTypeTestCase {
            description: "a native type warns when the destinations differ",
            text: "native('GEOGRAPHY')",
            destinations: MIXED,
            expected_type: ColumnType::Native("GEOGRAPHY".to_owned()),
            expected_written_as: None,
            expected_warnings: 1,
        },
        ParseTypeTestCase {
            description: "one engine twice counts once for native types",
            text: "native(\"HUGEINT\")",
            destinations: &[Warehouse::DuckDb, Warehouse::DuckDb],
            expected_type: ColumnType::Native("HUGEINT".to_owned()),
            expected_written_as: None,
            expected_warnings: 0,
        },
    ];
    for test_case in test_cases {
        let parsed = parse_type(test_case.text, test_case.destinations)
            .unwrap_or_else(|error| panic!("{}: {error}", test_case.description));
        assert_eq!(
            parsed.column_type, test_case.expected_type,
            "{}",
            test_case.description
        );
        assert_eq!(
            parsed.written_as.as_deref(),
            test_case.expected_written_as,
            "{}",
            test_case.description
        );
        assert_eq!(
            parsed.warnings.len(),
            test_case.expected_warnings,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_an_invalid_type_when_parsing_then_the_error_explains_the_fix() {
    let test_cases = [
        ParseTypeErrorTestCase {
            description: "a misspelt name suggests the nearest portable name",
            text: "decmal(18,2)",
            destinations: MIXED,
            expected_message: "did you mean \"decimal(p,s)\"",
        },
        ParseTypeErrorTestCase {
            description: "a misspelt timestamp suggests a portable timestamp",
            text: "timestamp_tzz",
            destinations: MIXED,
            expected_message: "did you mean \"timestamp_tz\"",
        },
        ParseTypeErrorTestCase {
            description: "an array holds scalars only, not json",
            text: "array(json)",
            destinations: MIXED,
            expected_message: "cannot hold json elements",
        },
        ParseTypeErrorTestCase {
            description: "BigQuery's write API refuses arrays of zoned timestamps",
            text: "array(timestamp_tz)",
            destinations: &[Warehouse::BigQuery],
            expected_message: "pin array(timestamp_ntz), which holds UTC times, or json",
        },
        ParseTypeErrorTestCase {
            description: "arrays do not nest",
            text: "array(array(int))",
            destinations: MIXED,
            expected_message: "cannot hold array(int) elements",
        },
        ParseTypeErrorTestCase {
            description: "an array's element type is checked like any type",
            text: "array(intt)",
            destinations: MIXED,
            expected_message: "its element type: unknown column type \"intt\"",
        },
        ParseTypeErrorTestCase {
            description: "a bare timestamp is ambiguous",
            text: "TIMESTAMP",
            destinations: SNOWFLAKE,
            expected_message: "use \"timestamp_tz\"",
        },
        ParseTypeErrorTestCase {
            description: "a bare timestamp with a precision is ambiguous too",
            text: "timestamp(6)",
            destinations: SNOWFLAKE,
            expected_message: "\"timestamp_ntz\" (without)",
        },
        ParseTypeErrorTestCase {
            description: "a decimal needs a precision and scale",
            text: "decimal",
            destinations: MIXED,
            expected_message: "needs a precision and scale",
        },
        ParseTypeErrorTestCase {
            description: "a precision above 38 is rejected",
            text: "decimal(40,2)",
            destinations: MIXED,
            expected_message: "precision must be between 1 and 38",
        },
        ParseTypeErrorTestCase {
            description: "a scale above the precision is rejected",
            text: "decimal(4,6)",
            destinations: MIXED,
            expected_message: "scale cannot be larger",
        },
        ParseTypeErrorTestCase {
            description: "a zero varchar length is rejected",
            text: "varchar(0)",
            destinations: MIXED,
            expected_message: "must be a positive whole number",
        },
        ParseTypeErrorTestCase {
            description: "a Snowflake name is rejected when another destination differs",
            text: "VARIANT",
            destinations: MIXED,
            expected_message: "is a Snowflake type, but this table also loads into DuckDB; use the portable name \"json\"",
        },
        ParseTypeErrorTestCase {
            description: "a Postgres name names the portable replacement",
            text: "jsonb",
            destinations: SNOWFLAKE,
            expected_message: "use the portable name \"json\"",
        },
        ParseTypeErrorTestCase {
            description: "a session-dependent timestamp is rejected",
            text: "TIMESTAMP_LTZ",
            destinations: SNOWFLAKE,
            expected_message: "depends on each session's time zone",
        },
        ParseTypeErrorTestCase {
            description: "a 32-bit float is rejected",
            text: "real",
            destinations: DUCKDB,
            expected_message: "use \"float\"",
        },
        ParseTypeErrorTestCase {
            description: "an unclosed parenthesis is malformed",
            text: "decimal(18,2",
            destinations: MIXED,
            expected_message: "without a closing one",
        },
        ParseTypeErrorTestCase {
            description: "a plain name with arguments is malformed",
            text: "bool(1)",
            destinations: MIXED,
            expected_message: "takes no arguments",
        },
        ParseTypeErrorTestCase {
            description: "a native type must be quoted",
            text: "native(GEOGRAPHY)",
            destinations: SNOWFLAKE,
            expected_message: "needs a quoted warehouse type",
        },
    ];
    for test_case in test_cases {
        let error = parse_type(test_case.text, test_case.destinations)
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(
            error.contains(test_case.expected_message),
            "{}: {error}",
            test_case.description
        );
    }
}
