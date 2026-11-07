use crate::adapters::bigquery::models::BigQuerySql;
use crate::adapters::bigquery::tests::test_types::RenderedTestCase;
use crate::adapters::contract::models::{CursorValue, TableName};
use crate::adapters::contract::types::SqlAdapter;

/// `` `p`.`raw`.`orders` ``'s name.
fn orders() -> TableName {
    TableName {
        database: Some("p".to_owned()),
        schema: "raw".to_owned(),
        table: "orders".to_owned(),
    }
}

#[test]
fn given_bigquery_sql_when_rendering_names_options_and_queries_then_googlesql_reads_them() {
    let adapter = BigQuerySql;
    let build = TableName {
        table: "orders__sqlload_staging_load1".to_owned(),
        ..orders()
    };
    let test_cases = [
        RenderedTestCase {
            description: "an identifier escapes backticks and backslashes with a backslash",
            rendered: vec![adapter.render_identifier("a`b\\c")],
            expected: &["`a\\`b\\\\c`"],
        },
        RenderedTestCase {
            description: "a description is a table option, since BigQuery has no COMMENT ON",
            rendered: adapter
                .render_table_comment(&orders(), "Orders, it's said")
                .into_iter()
                .collect(),
            expected: &[
                "ALTER TABLE `p`.`raw`.`orders` SET OPTIONS (description = 'Orders, it\\'s said')",
            ],
        },
        RenderedTestCase {
            description: "the build table replaces the table in one statement, then is dropped",
            rendered: adapter.render_swap(&orders(), &build, &[]),
            expected: &[
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
            expected: &[
                "CAST(UNIX_MICROS(MAX(c)) AS STRING)",
                "CAST(UNIX_MICROS(TIMESTAMP(MAX(c), 'UTC')) AS STRING)",
                "FORMAT_DATE('%F', MAX(c))",
            ],
        },
        RenderedTestCase {
            description: "stale staging tables are listed from the dataset's own INFORMATION_SCHEMA, without ILIKE",
            rendered: vec![adapter.list_staging_relations_sql(&orders(), None)],
            expected: &[
                "SELECT table_name FROM `p`.`raw`.INFORMATION_SCHEMA.TABLES WHERE LOWER(table_name) LIKE '%__sqlload_staging_%'",
            ],
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            test_case.rendered, test_case.expected,
            "{}",
            test_case.description
        );
    }
}
