use crate::adapters::bigquery::models::BigQuerySql;
use crate::adapters::bigquery::tests::helpers::orders;
use crate::adapters::bigquery::tests::test_types::RenderedTestCase;
use crate::adapters::contract::models::{
    CursorValue, MergeDedupe, TableName, WriteMode, WriteStatementsRequest,
};
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;
use crate::schema_plan::models::{ColumnChange, ColumnOrigin, DesiredColumn, LiveColumn};

#[test]
fn given_bigquery_sql_when_rendering_names_options_and_queries_then_googlesql_reads_them() {
    let adapter = BigQuerySql;
    let build = TableName {
        table: "orders__sqlload_staging_load1".to_owned(),
        ..orders()
    };
    let keyed = [
        DesiredColumn {
            name: "x".to_owned(),
            column_type: ColumnType::Float,
            origin: ColumnOrigin::Source,
        },
        DesiredColumn {
            name: "doc".to_owned(),
            column_type: ColumnType::Json,
            origin: ColumnOrigin::Source,
        },
    ];
    let deduped = WriteMode::Merge {
        unique_key: vec!["x".to_owned(), "doc".to_owned()],
        not_null_keys: Vec::new(),
        dedupe: Some(MergeDedupe {
            ordinal: "_sqlload_row".to_owned(),
            newest_by: Vec::new(),
        }),
    };
    let test_cases = [
        RenderedTestCase {
            description: "a merge keyed on FLOAT64 and JSON, which BigQuery refuses in PARTITION BY, ranks repeated keys by their exact text, -0 as 0, NULLs together, and keeps every NaN key, as MERGE matches none; keys that may be NULL match as `=` or both NULL, which BigQuery still joins by hash",
            rendered: vec![adapter.render_merge(
                &WriteStatementsRequest {
                    table: &orders(),
                    columns: &keyed,
                    mode: &deduped,
                    hashed: &[],
                    built: None,
                },
                &["x".to_owned(), "doc".to_owned()],
                "s",
                &["`x`".to_owned(), "PARSE_JSON(`doc`)".to_owned()],
            )],
            expected_statements: &[
                "MERGE INTO `p`.`raw`.`orders` AS __target USING (SELECT `x` AS `x`, `doc` AS `doc` FROM (SELECT `x`, `doc` FROM (SELECT `x` AS `x`, PARSE_JSON(`doc`) AS `doc`, ROW_NUMBER() OVER (PARTITION BY IF(IS_NAN(`x`), GENERATE_UUID(), FORMAT('%.17g', `x` + 0.0)), TO_JSON_STRING(PARSE_JSON(`doc`)) ORDER BY `_sqlload_row` DESC) AS __sqlload_rank FROM s) AS __sqlload_ranked WHERE __sqlload_rank = 1) AS __sqlload_kept) AS __source ON (__target.`x` = __source.`x` OR (__target.`x` IS NULL AND __source.`x` IS NULL)) AND (__target.`doc` = __source.`doc` OR (__target.`doc` IS NULL AND __source.`doc` IS NULL)) WHEN NOT MATCHED THEN INSERT (`x`, `doc`) VALUES (__source.`x`, __source.`doc`)",
            ],
        },
        RenderedTestCase {
            description: "an identifier escapes backticks and backslashes with a backslash",
            rendered: vec![adapter.render_identifier("a`b\\c")],
            expected_statements: &["`a\\`b\\\\c`"],
        },
        RenderedTestCase {
            description: "a rebuild lists every column with its type, so a decimal keeps its precision and scale through the query, and casts each decimal to the unparameterized type it is declared as, which a BIGNUMERIC(20,2) made outside SQLLoad also coerces to",
            rendered: adapter.render_change_columns(
                &orders(),
                &[ColumnChange::Rebuild {
                    column: "qty".to_owned(),
                    from: ColumnType::Int,
                    to: ColumnType::Float,
                    sql_type: "FLOAT64".to_owned(),
                }],
                &[
                    LiveColumn {
                        name: "amount".to_owned(),
                        column_type: ColumnType::Decimal {
                            precision: 20,
                            scale: 2,
                        },
                        fraction_digits: None,
                    },
                    LiveColumn {
                        name: "total".to_owned(),
                        column_type: ColumnType::Decimal {
                            precision: 40,
                            scale: 2,
                        },
                        fraction_digits: None,
                    },
                    LiveColumn {
                        name: "qty".to_owned(),
                        column_type: ColumnType::Int,
                        fraction_digits: None,
                    },
                ],
            ),
            expected_statements: &[
                "CREATE OR REPLACE TABLE `p`.`raw`.`orders` (`amount` NUMERIC(20,2), `total` BIGNUMERIC(40,2), `qty` FLOAT64) AS SELECT CAST(`amount` AS NUMERIC) AS `amount`, CAST(`total` AS BIGNUMERIC) AS `total`, CAST(`qty` AS FLOAT64) AS `qty` FROM `p`.`raw`.`orders`",
            ],
        },
        RenderedTestCase {
            description: "a table with a STRUCT column, whose type can't be named again, is rebuilt with the types its query gives",
            rendered: adapter.render_change_columns(
                &orders(),
                &[ColumnChange::Rebuild {
                    column: "qty".to_owned(),
                    from: ColumnType::Int,
                    to: ColumnType::Float,
                    sql_type: "FLOAT64".to_owned(),
                }],
                &[
                    LiveColumn {
                        name: "meta".to_owned(),
                        column_type: ColumnType::Native("STRUCT".to_owned()),
                        fraction_digits: None,
                    },
                    LiveColumn {
                        name: "qty".to_owned(),
                        column_type: ColumnType::Int,
                        fraction_digits: None,
                    },
                ],
            ),
            expected_statements: &[
                "CREATE OR REPLACE TABLE `p`.`raw`.`orders` AS SELECT `meta`, CAST(`qty` AS FLOAT64) AS `qty` FROM `p`.`raw`.`orders`",
            ],
        },
        RenderedTestCase {
            description: "a description is a table option, since BigQuery has no COMMENT ON",
            rendered: adapter
                .render_table_comment(&orders(), "Orders, it's said")
                .into_iter()
                .collect(),
            expected_statements: &[
                "ALTER TABLE `p`.`raw`.`orders` SET OPTIONS (description = 'Orders, it\\'s said')",
            ],
        },
        RenderedTestCase {
            description: "the build table's rows replace the table in one statement, its columns' precision, scale and length kept from the build table, then the build table is dropped",
            rendered: adapter.render_swap(&orders(), &build, &[]),
            expected_statements: &[
                "CREATE OR REPLACE TABLE `p`.`raw`.`orders` LIKE `p`.`raw`.`orders__sqlload_staging_load1` AS SELECT * FROM `p`.`raw`.`orders__sqlload_staging_load1`",
                "DROP TABLE IF EXISTS `p`.`raw`.`orders__sqlload_staging_load1`",
            ],
        },
        RenderedTestCase {
            description: "cursor maxima read as text whatever the session's time zone",
            rendered: vec![
                adapter.render_cursor_text("c", CursorValue::EpochMicros { zoned: true }),
                adapter.render_cursor_text("c", CursorValue::EpochMicros { zoned: false }),
                adapter.render_cursor_text("c", CursorValue::Date),
            ],
            expected_statements: &[
                "CAST(UNIX_MICROS(MAX(c)) AS STRING)",
                "CAST(UNIX_MICROS(TIMESTAMP(MAX(c), 'UTC')) AS STRING)",
                "FORMAT_DATE('%F', MAX(c))",
            ],
        },
        RenderedTestCase {
            description: "stale staging tables are listed from the dataset's own INFORMATION_SCHEMA, without ILIKE",
            rendered: vec![adapter.list_staging_relations_sql(&orders(), None)],
            expected_statements: &[
                "SELECT table_name FROM `p`.`raw`.INFORMATION_SCHEMA.TABLES WHERE LOWER(table_name) LIKE '%__sqlload_staging_%'",
            ],
        },
        RenderedTestCase {
            description: "an unnested table's child tables are listed from its dataset's own INFORMATION_SCHEMA, each marked by whether it has a parent-id column under either compat",
            rendered: vec![adapter.list_child_tables_sql(&orders())],
            expected_statements: &[
                "SELECT max(CASE WHEN column_name IN ('_sqlload_parent_id', '_dlt_parent_id') THEN '1' ELSE '0' END) || table_name FROM `p`.`raw`.INFORMATION_SCHEMA.COLUMNS WHERE table_schema = 'raw' AND left(table_name, 8) = 'orders__' GROUP BY table_name",
            ],
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            test_case.rendered, test_case.expected_statements,
            "{}",
            test_case.description
        );
    }
}
