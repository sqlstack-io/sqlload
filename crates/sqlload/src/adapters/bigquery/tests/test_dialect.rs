use crate::adapters::bigquery::models::BigQuerySql;
use crate::adapters::bigquery::tests::helpers::orders;
use crate::adapters::bigquery::tests::test_types::RenderedTestCase;
use crate::adapters::contract::models::{
    CursorValue, MergeDedupe, TableName, WriteMode, WriteStatementsRequest,
};
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};

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
        dedupe: Some(MergeDedupe {
            ordinal: "_sqlload_row".to_owned(),
            sort: None,
        }),
    };
    let test_cases = [
        RenderedTestCase {
            description: "a merge keyed on FLOAT64 and JSON, which BigQuery refuses in PARTITION BY, ranks repeated keys by their exact text, -0 as 0, and keeps every NaN key, as MERGE matches none",
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
                "MERGE INTO `p`.`raw`.`orders` AS __target USING (SELECT `x` AS `x`, `doc` AS `doc` FROM (SELECT `x`, `doc` FROM (SELECT `x` AS `x`, PARSE_JSON(`doc`) AS `doc`, ROW_NUMBER() OVER (PARTITION BY IF(IS_NAN(`x`), GENERATE_UUID(), FORMAT('%.17g', `x` + 0.0)), TO_JSON_STRING(PARSE_JSON(`doc`)) ORDER BY `_sqlload_row` DESC) AS __sqlload_rank FROM s) AS __sqlload_ranked WHERE __sqlload_rank = 1 OR `x` IS NULL OR `doc` IS NULL) AS __sqlload_kept) AS __source ON __target.`x` = __source.`x` AND __target.`doc` = __source.`doc` WHEN NOT MATCHED THEN INSERT (`x`, `doc`) VALUES (__source.`x`, __source.`doc`)",
            ],
        },
        RenderedTestCase {
            description: "an identifier escapes backticks and backslashes with a backslash",
            rendered: vec![adapter.render_identifier("a`b\\c")],
            expected_statements: &["`a\\`b\\\\c`"],
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
            description: "the build table replaces the table in one statement, then is dropped",
            rendered: adapter.render_swap(&orders(), &build, &[]),
            expected_statements: &[
                "CREATE OR REPLACE TABLE `p`.`raw`.`orders` AS SELECT * FROM `p`.`raw`.`orders__sqlload_staging_load1`",
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
            description: "an unnested table's child tables are listed from its dataset's own INFORMATION_SCHEMA, each marked by whether it has the parent-id column",
            rendered: vec![adapter.list_child_tables_sql(&orders(), "_dlt_parent_id")],
            expected_statements: &[
                "SELECT max(CASE WHEN column_name = '_dlt_parent_id' THEN '1' ELSE '0' END) || table_name FROM `p`.`raw`.INFORMATION_SCHEMA.COLUMNS WHERE table_schema = 'raw' AND left(table_name, 8) = 'orders__' GROUP BY table_name",
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
