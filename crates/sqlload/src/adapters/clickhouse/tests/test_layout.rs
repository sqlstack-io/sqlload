use std::collections::BTreeMap;

use crate::adapters::clickhouse::_helpers::dialect::layout::{layout_clauses, layout_problems};
use crate::adapters::clickhouse::tests::test_types::LayoutTestCase;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::models::Warehouse;
use crate::loader::models::TableLayout;
use crate::project::models::LayoutSetting;

#[test]
fn given_a_table_layout_when_creating_a_clickhouse_table_then_its_engine_keys_and_settings_follow_it()
 {
    let test_cases = [
        LayoutTestCase {
            description: "no layout and no key: an unsorted MergeTree table, needing no nullable-key setting",
            layout: TableLayout::default(),
            unique_key: &[],
            expected_clauses: "ENGINE = MergeTree ORDER BY tuple()",
        },
        LayoutTestCase {
            description: "no order_by: sorted by the unique key, whose Nullable columns need allow_nullable_key",
            layout: TableLayout::default(),
            unique_key: &["id", "Region"],
            expected_clauses: "ENGINE = MergeTree ORDER BY (`id`, `Region`) SETTINGS allow_nullable_key = 1",
        },
        LayoutTestCase {
            description: "every field as written, expressions unquoted; an explicit allow_nullable_key is kept as set",
            layout: TableLayout {
                engine: Some("ReplacingMergeTree(_sqlload_loaded_at)".to_owned()),
                order_by: Some(vec!["event_id".to_owned(), "toDate(event_time)".to_owned()]),
                partition_by: Some("toYYYYMM(event_date)".to_owned()),
                primary_key: Some(vec!["event_id".to_owned()]),
                table_settings: BTreeMap::from([
                    ("index_granularity".to_owned(), LayoutSetting::Integer(4096)),
                    ("allow_nullable_key".to_owned(), LayoutSetting::Bool(false)),
                ]),
            },
            unique_key: &["id"],
            expected_clauses: "ENGINE = ReplacingMergeTree(_sqlload_loaded_at) ORDER BY (event_id, toDate(event_time)) PARTITION BY toYYYYMM(event_date) PRIMARY KEY (event_id) SETTINGS allow_nullable_key = 0, index_granularity = 4096",
        },
        LayoutTestCase {
            description: "a partition alone needs the nullable-key setting too; a text setting is quoted with backslash escapes",
            layout: TableLayout {
                partition_by: Some("toYYYYMM(day)".to_owned()),
                table_settings: BTreeMap::from([(
                    "storage_policy".to_owned(),
                    LayoutSetting::Text("it's".to_owned()),
                )]),
                ..TableLayout::default()
            },
            unique_key: &[],
            expected_clauses: "ENGINE = MergeTree ORDER BY tuple() PARTITION BY toYYYYMM(day) SETTINGS allow_nullable_key = 1, storage_policy = 'it\\'s'",
        },
    ];
    for test_case in test_cases {
        let unique_key: Vec<String> = test_case
            .unique_key
            .iter()
            .map(|key| (*key).to_owned())
            .collect();
        assert_eq!(
            layout_clauses(&test_case.layout, &unique_key),
            test_case.expected_clauses,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_layout_when_planning_then_only_clickhouse_takes_it_and_only_with_a_mergetree_engine() {
    let engine = |name: &str| TableLayout {
        engine: Some(name.to_owned()),
        ..TableLayout::default()
    };
    assert_eq!(
        layout_problems(&engine("ReplacingMergeTree(version)")),
        Vec::<String>::new()
    );
    assert_eq!(
        layout_problems(&engine("SharedMergeTree")),
        Vec::<String>::new()
    );
    for refused in [
        "ReplicatedMergeTree('/t', '{replica}')",
        "Log",
        "Distributed(c, d, t)",
    ] {
        assert_eq!(layout_problems(&engine(refused)).len(), 1, "{refused}");
    }
    let partitioned = TableLayout {
        partition_by: Some("toYYYYMM(day)".to_owned()),
        ..TableLayout::default()
    };
    assert_eq!(
        sql_adapter(Warehouse::Snowflake).layout_problems(&partitioned),
        vec!["engine, order_by, partition_by, primary_key and table_settings lay out ClickHouse tables; Snowflake tables take none".to_owned()]
    );
    assert_eq!(
        sql_adapter(Warehouse::Snowflake).layout_problems(&TableLayout::default()),
        Vec::<String>::new()
    );
}
