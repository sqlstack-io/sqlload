use sqlload::databricks_api::main::execute_statement::execute_statement;

use crate::databricks_helpers::{
    copy_fails, databricks_columns, expired_token, live_databricks, live_databricks_client,
    nothing_in_databricks, poll_refused, reset_live_databricks, run_databricks,
    run_live_databricks, slow_paged_answers, stale_and_running_loads,
};
use crate::helpers::{found, not_found, run_async, two_batches, two_days};
use crate::test_types::{DatabricksAnswerTestCase, DatabricksLoadTestCase, LiveDatabricksTestCase};

const STAGED: &str = "/Volumes/main/raw/stage/sqlload/";
const COPY_ORDERS: &str = "COPY INTO `main`.`raw`.`orders` FROM (SELECT ";

const LIVE_APPEND: LiveDatabricksTestCase = LiveDatabricksTestCase {
    description: "",
    source: "orders_append",
    settings: "write_strategy = \"append\"",
    file_set: two_days,
    expected_rows: "6",
};

const BASE: DatabricksLoadTestCase = DatabricksLoadTestCase {
    description: "",
    settings: "write_strategy = \"append\"",
    file_set: two_days,
    prepare: nothing_in_databricks,
    expected_error: "",
    expected_plan: &[],
    expected_sent: &[],
    expected_unsent: &[],
    expected_rows: None,
    expected_copies: 1,
    expected_left: &[],
    expected_cancelled: 0,
};

#[test]
fn given_a_databricks_target_when_loading_files_then_each_batch_is_staged_copied_and_removed()
-> Result<(), String> {
    let test_cases = [
        DatabricksLoadTestCase {
            description: "append: one COPY straight into the table, nothing left staged",
            expected_plan: &[
                "read parquet into Arrow -> Parquet -> PUT to /Volumes/main/raw/stage -> COPY INTO",
            ],
            expected_sent: &[COPY_ORDERS, "FROM '/Volumes/main/raw/stage/sqlload/"],
            expected_unsent: &["BEGIN", "COMMIT", "INSERT INTO `main`.`raw`.`orders`"],
            expected_rows: Some(6),
            ..BASE
        },
        DatabricksLoadTestCase {
            description: "append over two batches: one COPY per batch, each batch's files removed",
            file_set: two_batches,
            expected_rows: Some(2002),
            expected_copies: 2,
            ..BASE
        },
        DatabricksLoadTestCase {
            description: "replace: COPY into a build table, which replaces the table in one statement (the one-copy exception)",
            settings: "write_strategy = \"replace\"",
            expected_sent: &[
                "CREATE OR REPLACE TABLE `main`.`raw`.`orders` AS SELECT * FROM `main`.`raw`.`orders__sqlload_staging_",
                "COPY INTO `main`.`raw`.`orders__sqlload_staging_",
                "COMMENT ON TABLE `main`.`raw`.`orders` IS",
            ],
            expected_unsent: &[COPY_ORDERS, "INSERT OVERWRITE"],
            expected_rows: Some(6),
            ..BASE
        },
        DatabricksLoadTestCase {
            description: "delete_insert: COPY into staging, then one MERGE inserting every staged row and deleting the window",
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"day\"",
            expected_sent: &[
                "ON FALSE WHEN NOT MATCHED THEN INSERT",
                "WHEN NOT MATCHED BY SOURCE AND __target.`day` IN (CAST('2026-11-01' AS DATE), CAST('2026-11-02' AS DATE)) THEN DELETE",
            ],
            expected_unsent: &[
                COPY_ORDERS,
                "DELETE FROM `main`.`raw`.`orders`",
                "REPLACE WHERE",
            ],
            ..BASE
        },
        DatabricksLoadTestCase {
            description: "a COPY the warehouse refuses fails the load and still removes the batch's files",
            prepare: copy_fails,
            expected_error: "staged file refused",
            expected_rows: Some(0),
            expected_copies: 0,
            ..BASE
        },
        DatabricksLoadTestCase {
            description: "a refused token is fetched again once and the load goes on",
            prepare: expired_token,
            expected_rows: Some(6),
            ..BASE
        },
        DatabricksLoadTestCase {
            description: "a failed run's folder over a day old is removed; a load still writing keeps its folder, and a load that starts writing into an empty folder after cleanup listed it keeps its file",
            prepare: stale_and_running_loads,
            expected_rows: Some(6),
            expected_left: &[
                "/Volumes/main/raw/stage/sqlload/running-load",
                "/Volumes/main/raw/stage/sqlload/running-load/00001",
                "/Volumes/main/raw/stage/sqlload/running-load/00001/part-00001.parquet",
                "/Volumes/main/raw/stage/sqlload/starting-load",
                "/Volumes/main/raw/stage/sqlload/starting-load/00001",
                "/Volumes/main/raw/stage/sqlload/starting-load/00001/part-00001.parquet",
            ],
            ..BASE
        },
        DatabricksLoadTestCase {
            description: "statements answered after a poll (the first poll failing with 503 and retried), results in one-row chunks and listings in one-entry pages load and clean up the same",
            prepare: slow_paged_answers,
            expected_rows: Some(6),
            expected_left: &[
                "/Volumes/main/raw/stage/sqlload/running-load",
                "/Volumes/main/raw/stage/sqlload/running-load/00001",
                "/Volumes/main/raw/stage/sqlload/running-load/00001/part-00001.parquet",
                "/Volumes/main/raw/stage/sqlload/starting-load",
                "/Volumes/main/raw/stage/sqlload/starting-load/00001",
                "/Volumes/main/raw/stage/sqlload/starting-load/00001/part-00001.parquet",
            ],
            ..BASE
        },
        DatabricksLoadTestCase {
            description: "a poll refused outright fails the load and cancels the statement it stopped waiting for",
            prepare: poll_refused,
            expected_error: "poll failed",
            expected_copies: 0,
            expected_cancelled: 1,
            ..BASE
        },
    ];
    run_async(async {
        for test_case in &test_cases {
            let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
            let outcome = run_databricks(directory.path(), test_case).await?;
            let failed = !test_case.expected_error.is_empty();
            assert!(
                outcome.error.contains(test_case.expected_error)
                    && failed != outcome.error.is_empty(),
                "{}: {}\n{}",
                test_case.description,
                outcome.error,
                outcome.plan
            );
            assert_eq!(
                not_found(std::slice::from_ref(&outcome.plan), test_case.expected_plan),
                Vec::<&str>::new(),
                "{}: {}",
                test_case.description,
                outcome.plan
            );
            assert_eq!(
                not_found(&outcome.sent, test_case.expected_sent),
                Vec::<&str>::new(),
                "{}: {:#?}",
                test_case.description,
                outcome.sent
            );
            assert_eq!(
                found(&outcome.sent, test_case.expected_unsent),
                Vec::<&str>::new(),
                "{}: {:#?}",
                test_case.description,
                outcome.sent
            );
            assert_eq!(
                test_case.expected_rows.unwrap_or(outcome.rows),
                outcome.rows,
                "{}",
                test_case.description
            );
            assert_eq!(
                outcome.copies, test_case.expected_copies,
                "{}",
                test_case.description
            );
            assert_eq!(
                outcome.cancelled, test_case.expected_cancelled,
                "{}",
                test_case.description
            );
            let mut expected_left: Vec<String> = test_case
                .expected_left
                .iter()
                .map(|path| (*path).to_owned())
                .collect();
            expected_left.sort();
            let mut left = outcome.left.clone();
            left.sort();
            assert_eq!(
                left, expected_left,
                "{}: under {STAGED}",
                test_case.description
            );
        }
        Ok(())
    })
}

/// Skipped unless `SQLLOAD_DATABRICKS_SCHEMA` names a `sqlload_` schema, which it drops first, loads into, and drops at the end; a failed run leaves it for the next run to drop.
#[test]
fn given_a_live_databricks_workspace_when_loading_with_each_strategy_then_rows_land_and_nothing_stays_staged()
-> Result<(), String> {
    let Some(live) = live_databricks()? else {
        return Ok(());
    };
    let test_cases = [
        LiveDatabricksTestCase {
            description: "append",
            ..LIVE_APPEND
        },
        LiveDatabricksTestCase {
            description: "append again: the files are loaded, so nothing new",
            ..LIVE_APPEND
        },
        LiveDatabricksTestCase {
            description: "append over two batches: two COPYs",
            source: "orders_batches",
            file_set: two_batches,
            expected_rows: "2002",
            ..LIVE_APPEND
        },
        LiveDatabricksTestCase {
            description: "replace",
            source: "orders_replace",
            settings: "write_strategy = \"replace\"",
            ..LIVE_APPEND
        },
        LiveDatabricksTestCase {
            description: "replace again: the same rows, not twice",
            source: "orders_replace",
            settings: "write_strategy = \"replace\"",
            ..LIVE_APPEND
        },
        LiveDatabricksTestCase {
            description: "delete_insert on the day partition",
            source: "orders_days",
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"day\"",
            ..LIVE_APPEND
        },
        LiveDatabricksTestCase {
            description: "delete_insert again: the last day is read again and its MERGE deletes the rows it reinserts, so they are not doubled",
            source: "orders_days",
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"day\"",
            ..LIVE_APPEND
        },
    ];
    run_async(async {
        let api = live_databricks_client(&live)?;
        reset_live_databricks(&api, &live).await?;
        let schema = format!("`{}`.`{}`", live.catalog, live.schema);
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        for test_case in &test_cases {
            let outcome = run_live_databricks(directory.path(), &live, test_case).await?;
            assert_eq!(
                outcome,
                (String::new(), Some(test_case.expected_rows.to_owned()), 0),
                "{}: the error, the rows and the load folders left in the volume",
                test_case.description
            );
        }
        let dropped =
            execute_statement(&api, &format!("DROP SCHEMA IF EXISTS {schema} CASCADE")).await;
        dropped.map_err(|error| error.to_string())?;
        Ok(())
    })
}

#[test]
fn given_databricks_answers_after_polls_or_in_chunks_when_reading_them_then_every_row_arrives_in_order()
-> Result<(), String> {
    let test_cases = [
        DatabricksAnswerTestCase {
            description: "every row in the first chunk, answered at once",
            chunk_rows: None,
            pending_polls: 0,
            expected_columns: &["id", "amount", "note"],
        },
        DatabricksAnswerTestCase {
            description: "one row per chunk: the rest come through each chunk's link",
            chunk_rows: Some(1),
            pending_polls: 0,
            expected_columns: &["id", "amount", "note"],
        },
        DatabricksAnswerTestCase {
            description: "two rows then one, answered after two polls",
            chunk_rows: Some(2),
            pending_polls: 2,
            expected_columns: &["id", "amount", "note"],
        },
    ];
    run_async(async {
        for test_case in &test_cases {
            let columns = databricks_columns(test_case).await?;
            let names: Vec<&str> = columns
                .iter()
                .map(|name| name.as_deref().unwrap_or("NULL"))
                .collect();
            assert_eq!(
                names, test_case.expected_columns,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}
