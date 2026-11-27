use std::collections::BTreeMap;

use crate::adapters::bigquery::_helpers::dialect::layout::{layout_drift, layout_problems};
use crate::adapters::bigquery::models::BigQuerySql;
use crate::adapters::bigquery::tests::helpers::{desired, orders};
use crate::adapters::contract::models::{TableName, WriteMode, WriteStatementsRequest};
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;
use crate::loader::models::TableLayout;
use crate::project::models::LayoutSetting;

fn layout(partition_by: &str, cluster_by: &[&str], filtered: bool) -> TableLayout {
    TableLayout {
        partition_by: (!partition_by.is_empty()).then(|| partition_by.to_owned()),
        cluster_by: (!cluster_by.is_empty())
            .then(|| cluster_by.iter().map(|column| (*column).to_owned()).collect()),
        table_settings: match filtered {
            true => BTreeMap::from([(
                "require_partition_filter".to_owned(),
                LayoutSetting::Bool(true),
            )]),
            false => BTreeMap::new(),
        },
        ..TableLayout::default()
    }
}

fn build() -> TableName {
    TableName {
        table: "orders__sqlload_staging_load1".to_owned(),
        ..orders()
    }
}

fn keys(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

#[test]
fn given_a_partitioned_table_when_merging_then_only_a_key_holding_the_partition_column_prunes() {
    let columns = [
        desired("id", ColumnType::Int),
        desired("day", ColumnType::Date),
        desired("ts", ColumnType::TimestampTz),
        desired("n", ColumnType::Int),
    ];
    let merge = |key: &[&str]| WriteMode::Merge {
        unique_key: keys(key),
        not_null_keys: Vec::new(),
        dedupe: None,
    };
    let keyed_delete = |key: &[&str]| WriteMode::DeleteInsert {
        cursor_column: None,
        from: None,
        to: None,
        values: Vec::new(),
        replace_key: keys(key),
        not_null_keys: Vec::new(),
    };
    let test_cases: [(&str, &str, WriteMode, &[&str]); 6] = [
        (
            "a DATE column: the distinct staged days, and the table's NULL days, as a NULL key can match a NULL one",
            "day",
            merge(&["id", "day"]),
            &[
                "DECLARE __sqlload_partitions DEFAULT ARRAY(SELECT DISTINCT `__sqlload_partition_value` FROM (SELECT ",
                " AS __sqlload_partition_value FROM `p`.`raw`.`orders__sqlload_staging_load1`) WHERE __sqlload_partition_value IS NOT NULL); MERGE INTO `p`.`raw`.`orders` AS __target ",
                " AS __source ON (__target.`day` IN UNNEST(__sqlload_partitions) OR __target.`day` IS NULL) AND ",
            ],
        ),
        (
            "DATE of a timestamp: the staged rows' dates, compared with the table's rows' dates",
            "DATE(ts)",
            merge(&["ts", "id"]),
            &[
                "ARRAY(SELECT DISTINCT DATE(`__sqlload_partition_value`) FROM (SELECT ",
                " AS __source ON (DATE(__target.`ts`) IN UNNEST(__sqlload_partitions) OR __target.`ts` IS NULL) AND ",
            ],
        ),
        (
            "TIMESTAMP_TRUNC by month",
            "TIMESTAMP_TRUNC(ts, MONTH)",
            merge(&["id", "ts"]),
            &[
                "ARRAY(SELECT DISTINCT TIMESTAMP_TRUNC(`__sqlload_partition_value`, MONTH) FROM (SELECT ",
                " AS __source ON (TIMESTAMP_TRUNC(__target.`ts`, MONTH) IN UNNEST(__sqlload_partitions) OR __target.`ts` IS NULL) AND ",
            ],
        ),
        (
            "an integer range: the raw column between the staged minimum and maximum, as a filter on the bucket does not prune",
            "RANGE_BUCKET(n, GENERATE_ARRAY(0, 100, 10))",
            merge(&["id", "n"]),
            &[
                "DECLARE __sqlload_partitions DEFAULT (SELECT AS STRUCT MIN(__sqlload_partition_value) AS low, MAX(__sqlload_partition_value) AS high FROM (SELECT ",
                " AS __source ON (__target.`n` BETWEEN __sqlload_partitions.low AND __sqlload_partitions.high OR __target.`n` IS NULL) AND ",
            ],
        ),
        (
            "a delete_insert by a replace_key holding the partition column",
            "day",
            keyed_delete(&["day"]),
            &[
                "DECLARE __sqlload_partitions DEFAULT ARRAY(",
                " AS __source ON (__target.`day` IN UNNEST(__sqlload_partitions) OR __target.`day` IS NULL) AND __source.__sqlload_replaces AND ",
            ],
        ),
        (
            "a key without the partition column: no filter, as a key's old row may sit in a partition no staged row does",
            "day",
            merge(&["id"]),
            &["MERGE INTO `p`.`raw`.`orders` AS __target "],
        ),
    ];
    for (description, partition_by, mode, expected) in test_cases {
        let layout = layout(partition_by, &[], false);
        let statements = BigQuerySql.render_write(
            &WriteStatementsRequest {
                table: &orders(),
                columns: &columns,
                mode: &mode,
                hashed: &[],
                built: None,
                spared_load: None,
                layout: &layout,
            },
            &build(),
        );
        let missing: Vec<&&str> = expected
            .iter()
            .filter(|piece| !statements.iter().any(|statement| statement.contains(**piece)))
            .collect();
        let pruned = statements.iter().any(|statement| statement.contains("DECLARE"));
        assert_eq!(
            (statements.len(), missing, pruned),
            (1, Vec::<&&str>::new(), expected.len() > 1),
            "{description}: one statement, the pieces it lacks, whether it prunes\n{statements:#?}"
        );
    }
}

#[test]
fn given_a_layout_when_swapping_a_build_table_in_then_the_layout_and_its_filter_option_are_kept() {
    let mut relayout = layout("day", &["id"], true);
    relayout.relayout = true;
    let test_cases = [
        (
            "same layout: replaced LIKE the build table, the filter option added as the build table leaves it out",
            layout("day", &["id"], true),
            vec![
                "CREATE OR REPLACE TABLE `p`.`raw`.`orders` LIKE `p`.`raw`.`orders__sqlload_staging_load1` OPTIONS (require_partition_filter = TRUE) AS SELECT * FROM `p`.`raw`.`orders__sqlload_staging_load1`",
                "DROP TABLE IF EXISTS `p`.`raw`.`orders__sqlload_staging_load1`",
            ],
        ),
        (
            "a --reload laying the table out anew: dropped first, as CREATE OR REPLACE refuses another partitioning",
            relayout,
            vec![
                "DROP TABLE IF EXISTS `p`.`raw`.`orders`",
                "CREATE TABLE `p`.`raw`.`orders` LIKE `p`.`raw`.`orders__sqlload_staging_load1` OPTIONS (require_partition_filter = TRUE) AS SELECT * FROM `p`.`raw`.`orders__sqlload_staging_load1`",
                "DROP TABLE IF EXISTS `p`.`raw`.`orders__sqlload_staging_load1`",
            ],
        ),
    ];
    for (description, layout, expected) in test_cases {
        assert_eq!(
            BigQuerySql.render_swap(&orders(), &build(), &[], &layout),
            expected,
            "{description}"
        );
    }
}

#[test]
fn given_a_live_table_when_comparing_its_layout_with_the_config_then_any_difference_is_named() {
    let day_by_id = serde_json::json!({
        "timePartitioning": { "type": "DAY", "field": "Day" },
        "clustering": { "fields": ["id", "region"] },
    });
    let test_cases = [
        ("the same layout, names in another case", &day_by_id, layout("day", &["ID", "region"], false), None),
        (
            "DATE of a timestamp is a DAY partitioning of it",
            &serde_json::json!({ "timePartitioning": { "type": "DAY", "field": "ts" } }),
            layout("DATE(ts)", &[], false),
            None,
        ),
        (
            "clustering columns in another order cluster otherwise",
            &day_by_id,
            layout("day", &["region", "id"], false),
            Some("the table is partitioned by DAY of day, clustered by id, region, but its config lays it out partitioned by DAY of day, clustered by region, id"),
        ),
        (
            "a month partitioning where the config says day",
            &serde_json::json!({ "timePartitioning": { "type": "MONTH", "field": "day" } }),
            layout("day", &[], false),
            Some("the table is partitioned by MONTH of day, unclustered, but its config lays it out partitioned by DAY of day, unclustered"),
        ),
        (
            "an ingestion-time table, with its partition filter",
            &serde_json::json!({ "timePartitioning": { "type": "DAY" }, "requirePartitionFilter": true }),
            TableLayout::default(),
            Some("the table is partitioned by DAY of ingestion time, unclustered, require_partition_filter = true, but its config lays it out unpartitioned, unclustered"),
        ),
        (
            "an integer range differing in its interval, numbers as BigQuery sends them (text)",
            &serde_json::json!({ "rangePartitioning": { "field": "n", "range": { "start": "0", "end": "100", "interval": "20" } } }),
            layout("RANGE_BUCKET(n, GENERATE_ARRAY(0, 100, 10))", &[], false),
            Some("the table is partitioned by ranges of n from 0 to 100 by 20, unclustered, but its config lays it out partitioned by ranges of n from 0 to 100 by 10, unclustered"),
        ),
    ];
    for (description, found, layout, expected) in test_cases {
        assert_eq!(layout_drift(found, &layout).as_deref(), expected, "{description}");
    }
}

#[test]
fn given_a_bigquery_layout_when_checking_it_then_what_bigquery_refuses_is_named() {
    let engine = TableLayout {
        engine: Some("MergeTree".to_owned()),
        ..TableLayout::default()
    };
    let test_cases = [
        ("every form BigQuery takes", layout("DATETIME_TRUNC(at, HOUR)", &["a", "b", "c", "d"], true), Vec::<&str>::new()),
        ("ClickHouse's keys", engine, vec!["engine, order_by and primary_key lay out ClickHouse tables; a BigQuery table takes partition_by, cluster_by and table_settings.require_partition_filter"]),
        ("a bare expression", layout("toYYYYMM(day)", &[], false), vec!["partition_by \"toYYYYMM(day)\": BigQuery partitions by "]),
        ("a unit DATE_TRUNC doesn't take", layout("DATE_TRUNC(day, DAY)", &[], false), vec!["partition_by \"DATE_TRUNC(day, DAY)\": DATE_TRUNC partitions by MONTH, YEAR"]),
        ("five cluster columns", layout("", &["a", "b", "c", "d", "e"], false), vec!["cluster_by names 5 columns; BigQuery clusters by at most 4"]),
        ("a cluster expression", layout("", &["lower(a)"], false), vec!["cluster_by \"lower(a)\": BigQuery clusters by column names, not expressions"]),
        ("a partition filter without partitioning", layout("", &[], true), vec!["table_settings.require_partition_filter needs partition_by: an unpartitioned table has no partition to filter on"]),
    ];
    for (description, layout, expected) in test_cases {
        let problems = layout_problems(&layout);
        assert_eq!(problems.len(), expected.len(), "{description}: {problems:#?}");
        for (problem, wanted) in problems.iter().zip(&expected) {
            assert!(problem.starts_with(wanted), "{description}: {problem}");
        }
    }
}
