use crate::column_types::models::ColumnType;
use crate::snowflake_source::_helpers::catalog::{described_columns, listed_tables, table_matcher};
use crate::snowflake_source::constants::SHOW_ROWS_MAX;
use crate::snowflake_source::tests::helpers::sql_rows;
use crate::snowflake_source::tests::test_types::{DescribedColumnsTestCase, ListedTablesTestCase};

const TABLE_COLUMNS: &[&str] = &["name", "schema_name", "kind", "rows", "bytes", "comment"];
const VIEW_COLUMNS: &[&str] = &["name", "schema_name", "comment", "is_materialized"];

#[test]
fn given_show_listings_when_listing_tables_then_matching_tables_and_views_come_sorted_without_catalog_views()
 {
    let test_cases = [
        ListedTablesTestCase {
            description: "tables and views both match, by pattern ignoring case, minus excluded and catalog views, sorted",
            listings: vec![
                (
                    TABLE_COLUMNS,
                    vec![
                        vec![
                            Some("ORDERS"),
                            Some("MARTS"),
                            Some("TABLE"),
                            Some("120"),
                            Some("4096"),
                            Some("Order lines"),
                        ],
                        vec![
                            Some("EVENTS"),
                            Some("MARTS"),
                            Some("TABLE"),
                            Some("7"),
                            Some("512"),
                            Some(" "),
                        ],
                        vec![
                            Some("SCRATCH"),
                            Some("MARTS"),
                            Some("TABLE"),
                            Some("1"),
                            Some("1"),
                            None,
                        ],
                        vec![
                            Some("USERS"),
                            Some("RAW"),
                            Some("TABLE"),
                            Some("3"),
                            Some("64"),
                            None,
                        ],
                    ],
                ),
                (
                    VIEW_COLUMNS,
                    vec![
                        vec![Some("DAILY"), Some("MARTS"), Some("Per day"), Some("false")],
                        vec![
                            Some("TABLES"),
                            Some("INFORMATION_SCHEMA"),
                            Some("The tables"),
                            Some("false"),
                        ],
                    ],
                ),
            ],
            include: &["marts.*", "*.tables"],
            exclude: &["MARTS.SCRATCH"],
            expected_tables: Some(vec![
                ("MARTS.DAILY", -1, -1, Some("Per day")),
                ("MARTS.EVENTS", 7, 512, None),
                ("MARTS.ORDERS", 120, 4096, Some("Order lines")),
            ]),
        },
        ListedTablesTestCase {
            description: "a listing at the SHOW row cap may be cut short, so it is not trusted",
            listings: vec![(
                TABLE_COLUMNS,
                vec![vec![Some("T"), Some("S"), Some("TABLE"), None, None, None]; SHOW_ROWS_MAX],
            )],
            include: &["*"],
            exclude: &[],
            expected_tables: None,
        },
    ];
    for test_case in test_cases {
        let listings: Vec<_> = test_case
            .listings
            .iter()
            .map(|(columns, rows)| sql_rows(columns, rows))
            .collect();
        let patterns = |patterns: &[&str]| {
            table_matcher(
                &patterns
                    .iter()
                    .map(|pattern| (*pattern).to_owned())
                    .collect::<Vec<_>>(),
            )
            .expect("the test patterns are valid globs")
        };
        let listed = listed_tables(
            &listings,
            &patterns(test_case.include),
            &patterns(test_case.exclude),
        );
        let owned = listed.map(|tables| {
            tables
                .into_iter()
                .map(|table| {
                    (
                        format!("{}.{}", table.schema, table.name),
                        table.estimated_rows,
                        table.estimated_bytes,
                        table.comment,
                    )
                })
                .collect::<Vec<_>>()
        });
        let summary = owned.as_ref().map(|tables| {
            tables
                .iter()
                .map(|(name, rows, bytes, comment)| {
                    (name.as_str(), *rows, *bytes, comment.as_deref())
                })
                .collect::<Vec<_>>()
        });
        assert_eq!(
            summary, test_case.expected_tables,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_described_table_when_reading_columns_then_order_nullability_and_types_follow_describe() {
    let test_cases = [DescribedColumnsTestCase {
        description: "columns keep their order, NOT NULL and a collated text length",
        rows: &[
            ["ID", "NUMBER(18,0)", "N"],
            ["CODE", "VARCHAR(10) COLLATE 'en-ci'", "Y"],
        ],
        expected_columns: vec![
            ("id", false, Ok(ColumnType::Int)),
            ("code", true, Ok(ColumnType::Varchar(10))),
        ],
    }];
    for test_case in test_cases {
        let rows: Vec<Vec<Option<&str>>> = test_case
            .rows
            .iter()
            .map(|row| row.iter().map(|value| Some(*value)).collect())
            .collect();
        let described = described_columns(&sql_rows(&["name", "type", "null?"], &rows));
        let columns: Vec<(&str, bool, Result<ColumnType, String>)> = described
            .iter()
            .map(|column| {
                (
                    column.name.as_str(),
                    column.nullable,
                    column.column_type.clone(),
                )
            })
            .collect();
        assert_eq!(
            columns, test_case.expected_columns,
            "{}",
            test_case.description
        );
    }
}
