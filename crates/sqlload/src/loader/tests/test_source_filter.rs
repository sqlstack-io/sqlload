use crate::column_types::models::{ColumnType, Warehouse};
use crate::loader::_helpers::table_plan::source_filter;
use crate::loader::tests::test_types::SourceFilterTestCase;

#[test]
fn given_cursor_bounds_when_rendering_the_source_filter_then_literals_follow_the_cursor_type_and_the_source_dialect()
 {
    let test_cases = [
        SourceFilterTestCase {
            description: "a numeric cursor's bounds are bare numbers, which BigQuery compares with an INT64 column",
            cursor_type: ColumnType::Int,
            dialect: None,
            odbc: false,
            lower: Some((">", "41")),
            upper: Some("100.5"),
            expected_filter: Some("`id` > 41 AND `id` < 100.5"),
            expected_parameters: &[],
        },
        SourceFilterTestCase {
            description: "a text cursor holding digits stays a string",
            cursor_type: ColumnType::Text,
            dialect: None,
            odbc: false,
            lower: Some((">=", "41")),
            upper: None,
            expected_filter: Some("`id` >= '41'"),
            expected_parameters: &[],
        },
        SourceFilterTestCase {
            description: "a decimal cursor's bounds in BigQuery are BIGNUMERIC, not FLOAT64 literals that round",
            cursor_type: ColumnType::Decimal {
                precision: 38,
                scale: 9,
            },
            dialect: Some(Warehouse::BigQuery),
            odbc: false,
            lower: Some((">", "1234567890123.000000001")),
            upper: Some("-0.5"),
            expected_filter: Some(
                "`id` > CAST('1234567890123.000000001' AS BIGNUMERIC) AND `id` < CAST('-0.5' AS BIGNUMERIC)",
            ),
            expected_parameters: &[],
        },
        SourceFilterTestCase {
            description: "a decimal cursor's bounds elsewhere stay bare numbers",
            cursor_type: ColumnType::Decimal {
                precision: 38,
                scale: 9,
            },
            dialect: Some(Warehouse::Snowflake),
            odbc: false,
            lower: Some((">", "12.5")),
            upper: None,
            expected_filter: Some("`id` > 12.5"),
            expected_parameters: &[],
        },
        SourceFilterTestCase {
            description: "a BigQuery text bound escapes quotes and backslashes with a backslash, so a value cannot end the string",
            cursor_type: ColumnType::Text,
            dialect: Some(Warehouse::BigQuery),
            odbc: false,
            lower: Some((">", r"zz\' UNION ALL SELECT 1 --")),
            upper: Some(r"o'brien\"),
            expected_filter: Some(r"`id` > 'zz\\\' UNION ALL SELECT 1 --' AND `id` < 'o\'brien\\'"),
            expected_parameters: &[],
        },
        SourceFilterTestCase {
            description: "a Snowflake text maximum ending in a backslash doubles it, so it cannot escape the closing quote",
            cursor_type: ColumnType::Text,
            dialect: Some(Warehouse::Snowflake),
            odbc: false,
            lower: Some((">", r"o'brien\")),
            upper: None,
            expected_filter: Some(r"`id` > 'o''brien\\'"),
            expected_parameters: &[],
        },
        SourceFilterTestCase {
            description: "a Snowflake text bound holding a backslash before a quote stays one string, so the rest is not SQL",
            cursor_type: ColumnType::Text,
            dialect: Some(Warehouse::Snowflake),
            odbc: false,
            lower: Some((">=", r"zz\' OR 1=1 --")),
            upper: Some(r"zz\'"),
            expected_filter: Some(r"`id` >= 'zz\\'' OR 1=1 --' AND `id` < 'zz\\'''"),
            expected_parameters: &[],
        },
        SourceFilterTestCase {
            description: "a Postgres text bound doubles its quotes and keeps backslashes, as standard strings read them",
            cursor_type: ColumnType::Text,
            dialect: None,
            odbc: false,
            lower: Some((">", r"o'brien\")),
            upper: None,
            expected_filter: Some(r"`id` > 'o''brien\'"),
            expected_parameters: &[],
        },
        SourceFilterTestCase {
            description: "an ODBC text bound holding a quote and a backslash is a bound parameter, so no database's literal rules can misread it",
            cursor_type: ColumnType::Varchar(40),
            dialect: None,
            odbc: true,
            lower: Some((">", r"zz\' OR 1=1 --")),
            upper: Some(r"o'brien\"),
            expected_filter: Some("`id` > ? AND `id` < ?"),
            expected_parameters: &[r#"Text("zz\\' OR 1=1 --")"#, r#"Text("o'brien\\")"#],
        },
        SourceFilterTestCase {
            description: "an ODBC numeric bound stays a bare number, not a parameter",
            cursor_type: ColumnType::Int,
            dialect: None,
            odbc: true,
            lower: Some((">=", "41")),
            upper: None,
            expected_filter: Some("`id` >= 41"),
            expected_parameters: &[],
        },
        SourceFilterTestCase {
            description: "an ODBC timestamp cursor resumes after its millisecond maximum through timestamp parameters, which every driver compares as timestamps (OpenEdge compares no string with one)",
            cursor_type: ColumnType::TimestampNtz,
            dialect: None,
            odbc: true,
            lower: Some((">", "2026-01-31 23:59:59.997000")),
            upper: Some("2026-02-01 00:00:00.000000"),
            expected_filter: Some("`id` > ? AND `id` < ?"),
            expected_parameters: &[
                "Timestamp(2026-01-31T23:59:59.997)",
                "Timestamp(2026-02-01T00:00:00)",
            ],
        },
        SourceFilterTestCase {
            description: "an ODBC timestamp bound keeps microseconds rather than rounding them away",
            cursor_type: ColumnType::TimestampNtz,
            dialect: None,
            odbc: true,
            lower: Some((">=", "2026-01-31 23:59:59.123456")),
            upper: None,
            expected_filter: Some("`id` >= ?"),
            expected_parameters: &["Timestamp(2026-01-31T23:59:59.123456)"],
        },
        SourceFilterTestCase {
            description: "an ODBC date cursor's bound is a date parameter",
            cursor_type: ColumnType::Date,
            dialect: None,
            odbc: true,
            lower: Some((">=", "2026-02-01")),
            upper: None,
            expected_filter: Some("`id` >= ?"),
            expected_parameters: &["Date(2026-02-01)"],
        },
        SourceFilterTestCase {
            description: "a timestamp bound from any other source stays a string literal",
            cursor_type: ColumnType::TimestampNtz,
            dialect: None,
            odbc: false,
            lower: Some((">", "2026-01-31 23:59:59.997000")),
            upper: None,
            expected_filter: Some("`id` > '2026-01-31 23:59:59.997000'"),
            expected_parameters: &[],
        },
        SourceFilterTestCase {
            description: "an ODBC zoned timestamp bound stays a string literal, since an ODBC timestamp has no offset",
            cursor_type: ColumnType::TimestampTz,
            dialect: None,
            odbc: true,
            lower: Some((">", "2026-01-31 23:59:59.997000+00")),
            upper: None,
            expected_filter: Some("`id` > '2026-01-31 23:59:59.997000+00'"),
            expected_parameters: &[],
        },
        SourceFilterTestCase {
            description: "an ODBC timestamp bound that is not a timestamp stays one quoted string, never a timestamp parameter",
            cursor_type: ColumnType::TimestampNtz,
            dialect: None,
            odbc: true,
            lower: Some((">", "2026-01-31'} OR 1=1 --")),
            upper: None,
            expected_filter: Some("`id` > '2026-01-31''} OR 1=1 --'"),
            expected_parameters: &[],
        },
        SourceFilterTestCase {
            description: "a numeric cursor's bound that isn't a plain number is quoted, never written bare",
            cursor_type: ColumnType::Float,
            dialect: None,
            odbc: false,
            lower: Some((">=", "1 OR 1=1")),
            upper: Some("NaN"),
            expected_filter: Some("`id` >= '1 OR 1=1' AND `id` < 'NaN'"),
            expected_parameters: &[],
        },
        SourceFilterTestCase {
            description: "no bounds, no filter",
            cursor_type: ColumnType::Int,
            dialect: None,
            odbc: false,
            lower: None,
            upper: None,
            expected_filter: None,
            expected_parameters: &[],
        },
    ];
    for test_case in test_cases {
        let mut parameters = Vec::new();
        let filter = source_filter(
            "`id`",
            (Some(&test_case.cursor_type), test_case.dialect),
            (test_case.lower, test_case.upper),
            test_case.odbc.then_some(&mut parameters),
        );
        assert_eq!(
            (
                filter.as_deref(),
                parameters
                    .iter()
                    .map(|parameter| format!("{parameter:?}"))
                    .collect::<Vec<String>>()
            ),
            (
                test_case.expected_filter,
                test_case
                    .expected_parameters
                    .iter()
                    .map(|value| (*value).to_owned())
                    .collect::<Vec<String>>()
            ),
            "{}",
            test_case.description
        );
    }
}
