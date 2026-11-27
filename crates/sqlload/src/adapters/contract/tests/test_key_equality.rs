use crate::adapters::contract::models::{
    ChangeApply, ChangeWrite, WriteMode, WriteStatementsRequest,
};
use crate::adapters::contract::tests::helpers::{accounts, between, columns, mixed_merge};
use crate::adapters::contract::tests::test_types::KeyMatchTestCase;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::models::Warehouse;
use crate::loader::models::TableLayout;

#[test]
fn given_merge_keys_that_may_be_null_when_rendering_the_write_then_each_warehouse_matches_a_null_with_a_null()
 {
    let test_cases = [
        KeyMatchTestCase {
            description: "Snowflake compares a key that may be NULL with IS NOT DISTINCT FROM, and one never NULL with =",
            warehouse: Warehouse::Snowflake,
            mode: mixed_merge(),
            markers: (" AS __source ON ", " WHEN "),
            expected_merge: "__target.\"ID\" = __source.\"ID\" AND __target.\"UPDATED_AT\" IS NOT DISTINCT FROM __source.\"UPDATED_AT\"",
            expected_rekeyed: "__live.\"ID\" = __staged.\"ID\" AND __live.\"UPDATED_AT\" IS NOT DISTINCT FROM __staged.\"UPDATED_AT\"",
        },
        KeyMatchTestCase {
            description: "DuckDB (and MotherDuck) compare a key that may be NULL with IS NOT DISTINCT FROM",
            warehouse: Warehouse::DuckDb,
            mode: mixed_merge(),
            markers: (" AS __source ON ", " WHEN "),
            expected_merge: "__target.\"id\" = __source.\"id\" AND __target.\"updated_at\" IS NOT DISTINCT FROM __source.\"updated_at\"",
            expected_rekeyed: "__live.\"id\" = __staged.\"id\" AND __live.\"updated_at\" IS NOT DISTINCT FROM __staged.\"updated_at\"",
        },
        KeyMatchTestCase {
            description: "BigQuery compares a key that may be NULL as = or both NULL, which it still joins by hash, where IS NOT DISTINCT FROM would be a cross join",
            warehouse: Warehouse::BigQuery,
            mode: mixed_merge(),
            markers: (" AS __source ON ", " WHEN "),
            expected_merge: "__target.`id` = __source.`id` AND (__target.`updated_at` = __source.`updated_at` OR (__target.`updated_at` IS NULL AND __source.`updated_at` IS NULL))",
            expected_rekeyed: "__live.`id` = __staged.`id` AND (__live.`updated_at` = __staged.`updated_at` OR (__live.`updated_at` IS NULL AND __staged.`updated_at` IS NULL))",
        },
        KeyMatchTestCase {
            description: "Databricks compares a key that may be NULL with <=>",
            warehouse: Warehouse::Databricks,
            mode: mixed_merge(),
            markers: (" AS __source ON ", " WHEN "),
            expected_merge: "__target.`id` = __source.`id` AND __target.`updated_at` <=> __source.`updated_at`",
            expected_rekeyed: "__live.`id` = __staged.`id` AND __live.`updated_at` <=> __staged.`updated_at`",
        },
        KeyMatchTestCase {
            description: "ClickHouse replaces the rows whose keys its delta holds, a key that may be NULL listed as whether it is NULL and its value, as a mutation ignores transform_null_in; and joins it with isNotDistinctFrom",
            warehouse: Warehouse::ClickHouse,
            mode: mixed_merge(),
            markers: ("\"displaced\":\"", "\"}"),
            expected_merge: "(`id`, isNull(`updated_at`), ifNull(`updated_at`, defaultValueOfArgumentType(assumeNotNull(`updated_at`)))) IN (SELECT `id`, isNull(`updated_at`), ifNull(`updated_at`, defaultValueOfArgumentType(assumeNotNull(`updated_at`))) FROM `raw`.`app__accounts__sqlload_staging_load1_swap`)",
            expected_rekeyed: "__live.`id` = __staged.`id` AND isNotDistinctFrom(__live.`updated_at`, __staged.`updated_at`)",
        },
        KeyMatchTestCase {
            description: "Postgres upserts by ON CONFLICT on the key (its index matching NULLs), and joins a key that may be NULL as = or both NULL, which it can drive through an index where IS NOT DISTINCT FROM is a nested loop over every row",
            warehouse: Warehouse::Postgres,
            mode: mixed_merge(),
            markers: (" ON CONFLICT (", ")"),
            expected_merge: "\"id\", \"updated_at\"",
            expected_rekeyed: "__live.\"id\" = __staged.\"id\" AND (__live.\"updated_at\" = __staged.\"updated_at\" OR (__live.\"updated_at\" IS NULL AND __staged.\"updated_at\" IS NULL))",
        },
        KeyMatchTestCase {
            description: "a captured change's merge compares a key that may be NULL as a merge does",
            warehouse: Warehouse::Snowflake,
            mode: WriteMode::Changes(ChangeWrite {
                apply: ChangeApply::SoftDeletes,
                unique_key: vec!["id".to_owned()],
                not_null_keys: Vec::new(),
                truncated: None,
            }),
            markers: (" AS __source ON ", " WHEN "),
            expected_merge: "__target.\"ID\" IS NOT DISTINCT FROM __source.\"ID\"",
            expected_rekeyed: "",
        },
    ];
    for test_case in test_cases {
        let adapter = sql_adapter(test_case.warehouse);
        let table = accounts();
        let columns = columns();
        let (staging, _) = adapter.render_create_staging_table(&table, &columns, "load-1");
        let request = WriteStatementsRequest {
            table: &table,
            columns: &columns,
            mode: &test_case.mode,
            hashed: &[],
            layout: &TableLayout::default(),
            built: None,
            spared_load: None,
        };
        let written = adapter.render_write(&request, &staging).join("\n");
        let rekeyed = adapter
            .render_rekeyed_row_ids(&request, &staging, "_sqlload_row_id")
            .unwrap_or_default();
        assert_eq!(
            (
                between(&written, test_case.markers),
                between(&rekeyed, (" AS __staged ON ", " WHERE "))
            ),
            (
                test_case.expected_merge.to_owned(),
                test_case.expected_rekeyed.to_owned()
            ),
            "{}",
            test_case.description
        );
    }
}
