use crate::adapters::contract::models::CursorQuery;
use crate::adapters::contract::tests::helpers::accounts;
use crate::adapters::contract::tests::test_types::MaxCursorTestCase;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::models::{ColumnType, Warehouse};

#[test]
fn given_a_cursor_when_rendering_its_maximum_query_then_only_constant_columns_scope_it() {
    let test_cases = [
        MaxCursorTestCase {
            description: "without constant columns the whole table counts, as before",
            warehouse: Warehouse::DuckDb,
            scope: vec![],
            expected_query: "SELECT CAST(epoch_us(MAX(\"updated_at\")) AS VARCHAR) FROM \"raw\".\"app__accounts\"",
        },
        MaxCursorTestCase {
            description: "each constant column keeps only its value's rows, the literal cast to its type",
            warehouse: Warehouse::DuckDb,
            scope: vec![
                ("tenant_id".to_owned(), ColumnType::Text, "it's".to_owned()),
                ("shard".to_owned(), ColumnType::Int, "7".to_owned()),
            ],
            expected_query: "SELECT CAST(epoch_us(MAX(\"updated_at\")) AS VARCHAR) FROM \"raw\".\"app__accounts\" WHERE \"tenant_id\" = CAST('it''s' AS VARCHAR) AND \"shard\" = CAST('7' AS BIGINT)",
        },
        MaxCursorTestCase {
            description: "the scope takes each warehouse's quoting and types",
            warehouse: Warehouse::BigQuery,
            scope: vec![("tenant_id".to_owned(), ColumnType::Text, "a".to_owned())],
            expected_query: "SELECT CAST(UNIX_MICROS(MAX(`updated_at`)) AS STRING) FROM `raw`.`app__accounts` WHERE `tenant_id` = CAST('a' AS STRING)",
        },
    ];
    for test_case in test_cases {
        let query = sql_adapter(test_case.warehouse).render_max_cursor(&CursorQuery {
            table: &accounts(),
            column: "updated_at",
            column_type: &ColumnType::TimestampTz,
            lookback: None,
            scope: &test_case.scope,
        });
        assert_eq!(
            query.map(|read| read.sql).as_deref(),
            Some(test_case.expected_query),
            "{}",
            test_case.description
        );
    }
}
