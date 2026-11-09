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
            binds: false,
            lower: Some((">", "41")),
            upper: Some("100.5"),
            expected_filter: Some("`id` > 41 AND `id` < 100.5"),
            expected_parameters: &[],
        },
        SourceFilterTestCase {
            description: "a text cursor holding digits stays a string",
            cursor_type: ColumnType::Text,
            dialect: None,
            binds: false,
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
            binds: false,
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
            binds: false,
            lower: Some((">", "12.5")),
            upper: None,
            expected_filter: Some("`id` > 12.5"),
            expected_parameters: &[],
        },
        SourceFilterTestCase {
            description: "a BigQuery text bound escapes quotes and backslashes with a backslash, so a value cannot end the string",
            cursor_type: ColumnType::Text,
            dialect: Some(Warehouse::BigQuery),
            binds: false,
            lower: Some((">", r"zz\' UNION ALL SELECT 1 --")),
            upper: Some(r"o'brien\"),
            expected_filter: Some(r"`id` > 'zz\\\' UNION ALL SELECT 1 --' AND `id` < 'o\'brien\\'"),
            expected_parameters: &[],
        },
        SourceFilterTestCase {
            description: "a Snowflake text maximum ending in a backslash doubles it, so it cannot escape the closing quote",
            cursor_type: ColumnType::Text,
            dialect: Some(Warehouse::Snowflake),
            binds: false,
            lower: Some((">", r"o'brien\")),
            upper: None,
            expected_filter: Some(r"`id` > 'o''brien\\'"),
            expected_parameters: &[],
        },
        SourceFilterTestCase {
            description: "a Snowflake text bound holding a backslash before a quote stays one string, so the rest is not SQL",
            cursor_type: ColumnType::Text,
            dialect: Some(Warehouse::Snowflake),
            binds: false,
            lower: Some((">=", r"zz\' OR 1=1 --")),
            upper: Some(r"zz\'"),
            expected_filter: Some(r"`id` >= 'zz\\'' OR 1=1 --' AND `id` < 'zz\\'''"),
            expected_parameters: &[],
        },
        SourceFilterTestCase {
            description: "a Postgres text bound doubles its quotes and keeps backslashes, as standard strings read them",
            cursor_type: ColumnType::Text,
            dialect: None,
            binds: false,
            lower: Some((">", r"o'brien\")),
            upper: None,
            expected_filter: Some(r"`id` > 'o''brien\'"),
            expected_parameters: &[],
        },
        SourceFilterTestCase {
            description: "an ODBC text bound holding a quote and a backslash is a bound parameter, so no database's literal rules can misread it",
            cursor_type: ColumnType::Varchar(40),
            dialect: None,
            binds: true,
            lower: Some((">", r"zz\' OR 1=1 --")),
            upper: Some(r"o'brien\"),
            expected_filter: Some("`id` > ? AND `id` < ?"),
            expected_parameters: &[r"zz\' OR 1=1 --", r"o'brien\"],
        },
        SourceFilterTestCase {
            description: "an ODBC numeric bound stays a bare number, not a parameter",
            cursor_type: ColumnType::Int,
            dialect: None,
            binds: true,
            lower: Some((">=", "41")),
            upper: None,
            expected_filter: Some("`id` >= 41"),
            expected_parameters: &[],
        },
        SourceFilterTestCase {
            description: "a numeric cursor's bound that isn't a plain number is quoted, never written bare",
            cursor_type: ColumnType::Float,
            dialect: None,
            binds: false,
            lower: Some((">=", "1 OR 1=1")),
            upper: Some("NaN"),
            expected_filter: Some("`id` >= '1 OR 1=1' AND `id` < 'NaN'"),
            expected_parameters: &[],
        },
        SourceFilterTestCase {
            description: "no bounds, no filter",
            cursor_type: ColumnType::Int,
            dialect: None,
            binds: false,
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
            test_case.binds.then_some(&mut parameters),
        );
        assert_eq!(
            (filter.as_deref(), parameters),
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
