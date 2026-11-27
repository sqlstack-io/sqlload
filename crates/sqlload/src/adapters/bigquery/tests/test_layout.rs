use crate::adapters::bigquery::_helpers::dialect::layout::{
    layout_drift, layout_problems, layout_table_problems,
};
use crate::adapters::bigquery::models::BigQuerySql;
use crate::adapters::bigquery::tests::helpers::{
    desired, layout_of, merge_on, names, orders, orders_build,
};
use crate::adapters::bigquery::tests::test_types::{
    DriftTestCase, LayoutProblemsTestCase, PartitionFilterTestCase, PruneTestCase, RenderedTestCase,
};
use crate::adapters::contract::models::{WriteMode, WriteStatementsRequest};
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::loader::models::TableLayout;
use crate::project::models::WriteStrategy;
use crate::schema_plan::models::{ColumnChange, LiveColumn};

#[test]
fn given_a_partitioned_table_when_merging_then_only_a_key_holding_the_partition_column_prunes() {
    let columns = [
        desired("id", ColumnType::Int),
        desired("day", ColumnType::Date),
        desired("ts", ColumnType::TimestampTz),
        desired("n", ColumnType::Int),
    ];
    let test_cases = [
        PruneTestCase {
            description: "a DATE column: the distinct staged days, and the table's NULL days, as a NULL key can match a NULL one",
            partition_by: "day",
            mode: merge_on(&["id", "day"]),
            expected_pieces: &[
                "DECLARE __sqlload_partitions DEFAULT ARRAY(SELECT DISTINCT `__sqlload_partition_value` FROM (SELECT ",
                " AS __sqlload_partition_value FROM `p`.`raw`.`orders__sqlload_staging_load1`) WHERE __sqlload_partition_value IS NOT NULL); MERGE INTO `p`.`raw`.`orders` AS __target ",
                " AS __source ON (__target.`day` IN UNNEST(__sqlload_partitions) OR __target.`day` IS NULL) AND ",
            ],
            expected_pruned: true,
        },
        PruneTestCase {
            description: "DATE of a timestamp: the staged rows' dates, compared with the table's rows' dates",
            partition_by: "DATE(ts)",
            mode: merge_on(&["ts", "id"]),
            expected_pieces: &[
                "ARRAY(SELECT DISTINCT DATE(`__sqlload_partition_value`) FROM (SELECT ",
                " AS __source ON (DATE(__target.`ts`) IN UNNEST(__sqlload_partitions) OR __target.`ts` IS NULL) AND ",
            ],
            expected_pruned: true,
        },
        PruneTestCase {
            description: "TIMESTAMP_TRUNC by month",
            partition_by: "TIMESTAMP_TRUNC(ts, MONTH)",
            mode: merge_on(&["id", "ts"]),
            expected_pieces: &[
                "ARRAY(SELECT DISTINCT TIMESTAMP_TRUNC(`__sqlload_partition_value`, MONTH) FROM (SELECT ",
                " AS __source ON (TIMESTAMP_TRUNC(__target.`ts`, MONTH) IN UNNEST(__sqlload_partitions) OR __target.`ts` IS NULL) AND ",
            ],
            expected_pruned: true,
        },
        PruneTestCase {
            description: "an integer range: the raw column between the staged minimum and maximum, as a filter on the bucket does not prune",
            partition_by: "RANGE_BUCKET(n, GENERATE_ARRAY(0, 100, 10))",
            mode: merge_on(&["id", "n"]),
            expected_pieces: &[
                "DECLARE __sqlload_partitions DEFAULT (SELECT AS STRUCT MIN(__sqlload_partition_value) AS low, MAX(__sqlload_partition_value) AS high FROM (SELECT ",
                " AS __source ON (__target.`n` BETWEEN __sqlload_partitions.low AND __sqlload_partitions.high OR __target.`n` IS NULL) AND ",
            ],
            expected_pruned: true,
        },
        PruneTestCase {
            description: "a delete_insert by a replace_key holding the partition column",
            partition_by: "day",
            mode: WriteMode::DeleteInsert {
                cursor_column: None,
                from: None,
                to: None,
                values: Vec::new(),
                replace_key: names(&["day"]),
                not_null_keys: Vec::new(),
            },
            expected_pieces: &[
                " AS __source ON (__target.`day` IN UNNEST(__sqlload_partitions) OR __target.`day` IS NULL) AND __source.__sqlload_replaces AND ",
            ],
            expected_pruned: true,
        },
        PruneTestCase {
            description: "a key without the partition column: no filter, as a key's old row may sit in a partition no staged row does",
            partition_by: "day",
            mode: merge_on(&["id"]),
            expected_pieces: &["MERGE INTO `p`.`raw`.`orders` AS __target "],
            expected_pruned: false,
        },
    ];
    for test_case in test_cases {
        let layout = layout_of(test_case.partition_by, &[], false);
        let statements = BigQuerySql.render_write(
            &WriteStatementsRequest {
                table: &orders(),
                columns: &columns,
                mode: &test_case.mode,
                hashed: &[],
                built: None,
                spared_load: None,
                layout: &layout,
            },
            &orders_build(),
        );
        let missing: Vec<&&str> = test_case
            .expected_pieces
            .iter()
            .filter(|piece| {
                !statements
                    .iter()
                    .any(|statement| statement.contains(**piece))
            })
            .collect();
        let pruned = statements
            .iter()
            .any(|statement| statement.contains("DECLARE"));
        assert_eq!(
            (statements.len(), missing, pruned),
            (1, Vec::<&&str>::new(), test_case.expected_pruned),
            "{}: one statement, the pieces it lacks, whether it prunes\n{statements:#?}",
            test_case.description
        );
    }
}

#[test]
fn given_a_layout_when_rebuilding_the_table_then_the_layout_and_its_partition_filter_are_kept() {
    let relayout = TableLayout {
        relayout: true,
        ..layout_of("day", &["id"], true)
    };
    let live = [LiveColumn {
        name: "qty".to_owned(),
        column_type: ColumnType::Int,
        fraction_digits: None,
    }];
    let test_cases = [
        RenderedTestCase {
            description: "a swap of the same layout: replaced LIKE the unpartitioned build table, laid out here with its filter option",
            rendered: BigQuerySql.render_swap(
                &orders(),
                &orders_build(),
                &[],
                &layout_of("day", &["id"], true),
            ),
            expected_statements: &[
                "CREATE OR REPLACE TABLE `p`.`raw`.`orders` LIKE `p`.`raw`.`orders__sqlload_staging_load1` PARTITION BY `day` CLUSTER BY `id` OPTIONS (require_partition_filter = TRUE) AS SELECT * FROM `p`.`raw`.`orders__sqlload_staging_load1`",
                "DROP TABLE IF EXISTS `p`.`raw`.`orders__sqlload_staging_load1`",
            ],
        },
        RenderedTestCase {
            description: "a --reload laying the table out anew: renamed out of the way, as CREATE OR REPLACE refuses another partitioning, and dropped only once its successor holds the rows",
            rendered: BigQuerySql.render_swap(&orders(), &orders_build(), &[], &relayout),
            expected_statements: &[
                "ALTER TABLE `p`.`raw`.`orders` RENAME TO `orders__sqlload_relayout`",
                "CREATE TABLE `p`.`raw`.`orders` LIKE `p`.`raw`.`orders__sqlload_staging_load1` PARTITION BY `day` CLUSTER BY `id` OPTIONS (require_partition_filter = TRUE) AS SELECT * FROM `p`.`raw`.`orders__sqlload_staging_load1`",
                "DROP TABLE `p`.`raw`.`orders__sqlload_relayout`",
                "DROP TABLE IF EXISTS `p`.`raw`.`orders__sqlload_staging_load1`",
            ],
        },
        RenderedTestCase {
            description: "a build table is left unpartitioned, as the Storage Write API refuses rows of a date partition over 10 years back or a year ahead; the swap lays the table out",
            rendered: BigQuerySql.render_create_build_table(
                &orders_build(),
                &orders(),
                (&[desired("day", ColumnType::Date)], None, None),
            ),
            expected_statements: &[
                "CREATE OR REPLACE TABLE `p`.`raw`.`orders__sqlload_staging_load1` (`day` DATE)",
            ],
        },
        RenderedTestCase {
            description: "a type change's rewrite is laid out as the table is, reading every row with a filter its require_partition_filter takes",
            rendered: BigQuerySql.render_change_columns(
                &orders(),
                &[ColumnChange::Rebuild {
                    column: "qty".to_owned(),
                    from: ColumnType::Int,
                    to: ColumnType::Float,
                    sql_type: "FLOAT64".to_owned(),
                }],
                &live,
                &layout_of("day", &["id"], true),
            ),
            expected_statements: &[
                "CREATE OR REPLACE TABLE `p`.`raw`.`orders` (`qty` FLOAT64) PARTITION BY `day` CLUSTER BY `id` OPTIONS (require_partition_filter = TRUE) AS SELECT CAST(`qty` AS FLOAT64) AS `qty` FROM `p`.`raw`.`orders` WHERE (`day` IS NOT NULL OR `day` IS NULL)",
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

#[test]
fn given_a_live_table_when_comparing_its_layout_with_the_config_then_any_difference_is_named() {
    let day_by_id = serde_json::json!({
        "timePartitioning": { "type": "DAY", "field": "Day" },
        "clustering": { "fields": ["id", "region"] },
    });
    let test_cases = [
        DriftTestCase {
            description: "the same layout, names in another case",
            found: day_by_id.clone(),
            layout: layout_of("day", &["ID", "region"], false),
            expected_drift: None,
        },
        DriftTestCase {
            description: "DATE of a timestamp is a DAY partitioning of it",
            found: serde_json::json!({ "timePartitioning": { "type": "DAY", "field": "ts" } }),
            layout: layout_of("DATE(ts)", &[], false),
            expected_drift: None,
        },
        DriftTestCase {
            description: "clustering columns in another order cluster otherwise",
            found: day_by_id,
            layout: layout_of("day", &["region", "id"], false),
            expected_drift: Some(
                "the table is partitioned by DAY of day, clustered by id, region, but its config lays it out partitioned by DAY of day, clustered by region, id",
            ),
        },
        DriftTestCase {
            description: "a month partitioning where the config says day",
            found: serde_json::json!({ "timePartitioning": { "type": "MONTH", "field": "day" } }),
            layout: layout_of("day", &[], false),
            expected_drift: Some(
                "the table is partitioned by MONTH of day, unclustered, but its config lays it out partitioned by DAY of day, unclustered",
            ),
        },
        DriftTestCase {
            description: "an ingestion-time table, with its partition filter",
            found: serde_json::json!({ "timePartitioning": { "type": "DAY" }, "requirePartitionFilter": true }),
            layout: TableLayout::default(),
            expected_drift: Some(
                "the table is partitioned by DAY of ingestion time, unclustered, require_partition_filter = true, but its config lays it out unpartitioned, unclustered",
            ),
        },
        DriftTestCase {
            description: "an integer range differing in its interval, numbers as BigQuery sends them (text)",
            found: serde_json::json!({ "rangePartitioning": { "field": "n", "range": { "start": "0", "end": "100", "interval": "20" } } }),
            layout: layout_of("RANGE_BUCKET(n, GENERATE_ARRAY(0, 100, 10))", &[], false),
            expected_drift: Some(
                "the table is partitioned by ranges of n from 0 to 100 by 20, unclustered, but its config lays it out partitioned by ranges of n from 0 to 100 by 10, unclustered",
            ),
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            layout_drift(&test_case.found, &test_case.layout).as_deref(),
            test_case.expected_drift,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_bigquery_layout_when_checking_it_then_what_bigquery_refuses_is_named() {
    let test_cases = [
        LayoutProblemsTestCase {
            description: "every form BigQuery takes",
            layout: layout_of("DATETIME_TRUNC(at, HOUR)", &["a", "b", "c", "d"], true),
            expected_problems: &[],
        },
        LayoutProblemsTestCase {
            description: "ClickHouse's keys",
            layout: TableLayout {
                engine: Some("MergeTree".to_owned()),
                ..TableLayout::default()
            },
            expected_problems: &[
                "engine, order_by and primary_key lay out ClickHouse tables; a BigQuery table takes partition_by, cluster_by and table_settings.require_partition_filter",
            ],
        },
        LayoutProblemsTestCase {
            description: "a bare expression",
            layout: layout_of("toYYYYMM(day)", &[], false),
            expected_problems: &["partition_by \"toYYYYMM(day)\": BigQuery partitions by "],
        },
        LayoutProblemsTestCase {
            description: "a unit DATE_TRUNC doesn't take",
            layout: layout_of("DATE_TRUNC(day, DAY)", &[], false),
            expected_problems: &[
                "partition_by \"DATE_TRUNC(day, DAY)\": DATE_TRUNC partitions by MONTH, YEAR",
            ],
        },
        LayoutProblemsTestCase {
            description: "a range whose end is not past its start",
            layout: layout_of("RANGE_BUCKET(n, GENERATE_ARRAY(10, 10, 1))", &[], false),
            expected_problems: &[
                "partition_by \"RANGE_BUCKET(n, GENERATE_ARRAY(10, 10, 1))\": BigQuery partitions by ",
            ],
        },
        LayoutProblemsTestCase {
            description: "more ranges than BigQuery's 10,000 partitions",
            layout: layout_of("RANGE_BUCKET(n, GENERATE_ARRAY(0, 1000000, 1))", &[], false),
            expected_problems: &[
                "partition_by \"RANGE_BUCKET(n, GENERATE_ARRAY(0, 1000000, 1))\" makes 1000000 ranges; BigQuery takes at most 10000, so widen the interval",
            ],
        },
        LayoutProblemsTestCase {
            description: "exactly 10,000 ranges",
            layout: layout_of("RANGE_BUCKET(n, GENERATE_ARRAY(0, 100000, 10))", &[], false),
            expected_problems: &[],
        },
        LayoutProblemsTestCase {
            description: "five cluster columns",
            layout: layout_of("", &["a", "b", "c", "d", "e"], false),
            expected_problems: &["cluster_by names 5 columns; BigQuery clusters by at most 4"],
        },
        LayoutProblemsTestCase {
            description: "a cluster expression",
            layout: layout_of("", &["lower(a)"], false),
            expected_problems: &[
                "cluster_by \"lower(a)\": BigQuery clusters by column names, not expressions",
            ],
        },
        LayoutProblemsTestCase {
            description: "a partition filter without partitioning",
            layout: layout_of("", &[], true),
            expected_problems: &[
                "table_settings.require_partition_filter needs partition_by: an unpartitioned table has no partition to filter on",
            ],
        },
    ];
    for test_case in test_cases {
        let problems = layout_problems(&test_case.layout);
        let starts: Vec<bool> = problems
            .iter()
            .zip(test_case.expected_problems)
            .map(|(problem, wanted)| problem.starts_with(wanted))
            .collect();
        assert_eq!(
            (problems.len(), starts.contains(&false)),
            (test_case.expected_problems.len(), false),
            "{}: {problems:#?}",
            test_case.description
        );
    }
}

#[test]
fn given_require_partition_filter_when_planning_a_table_then_writes_that_cannot_filter_are_refused()
{
    let columns = [
        desired("id", ColumnType::Int),
        desired("day", ColumnType::Date),
    ];
    let test_cases = [
        PartitionFilterTestCase {
            description: "an append: every statement filters",
            strategy: WriteStrategy::Append,
            unique_key: &[],
            newest_by: &[],
            settings: "",
            expected_problems: &[],
        },
        PartitionFilterTestCase {
            description: "a merge on a key holding the partition column",
            strategy: WriteStrategy::Merge,
            unique_key: &["id", "day"],
            newest_by: &[],
            settings: "",
            expected_problems: &[],
        },
        PartitionFilterTestCase {
            description: "a merge on a key without it, whose MERGE BigQuery refuses without a partition filter in ON",
            strategy: WriteStrategy::Merge,
            unique_key: &["id"],
            newest_by: &[],
            settings: "",
            expected_problems: &[
                "require_partition_filter: a merge filters on the partition column only when unique_key holds it, and unique_key lacks day; set table_settings.require_partition_filter = false, or add day to unique_key",
            ],
        },
        PartitionFilterTestCase {
            description: "newest_by, whose read of the table's rows carries no filter",
            strategy: WriteStrategy::Merge,
            unique_key: &["id", "day"],
            newest_by: &["id"],
            settings: "",
            expected_problems: &[
                "require_partition_filter: newest_by reads the table's rows in queries SQLLoad can't filter on the partition column day; set table_settings.require_partition_filter = false, or leave newest_by out",
            ],
        },
        PartitionFilterTestCase {
            description: "checks.max_row_drop, whose count of the table's rows carries no filter",
            strategy: WriteStrategy::Replace,
            unique_key: &[],
            newest_by: &[],
            settings: "checks = { max_row_drop = \"20%\" }",
            expected_problems: &[
                "require_partition_filter: checks.max_row_drop reads the table's rows in queries SQLLoad can't filter on the partition column day; set table_settings.require_partition_filter = false, or leave checks.max_row_drop out",
            ],
        },
    ];
    for test_case in test_cases {
        let spec = crate::loader::models::TableSpec {
            layout: layout_of("day", &[], true),
            strategy: test_case.strategy,
            unique_key: names(test_case.unique_key),
            newest_by: names(test_case.newest_by),
            ..crate::contract::tests::helpers::postgres_spec(
                test_case.settings,
                Warehouse::BigQuery,
            )
            .unwrap_or_else(|error| panic!("{}: {error}", test_case.description))
        };
        assert_eq!(
            layout_table_problems(&spec, &columns),
            test_case.expected_problems,
            "{}",
            test_case.description
        );
    }
}
