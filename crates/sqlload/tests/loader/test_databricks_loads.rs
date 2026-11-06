use crate::helpers::{
    copy_fails, expired_token, found, not_found, nothing_in_databricks, run_async, run_databricks,
    stale_and_running_loads, two_batches, two_days,
};
use crate::test_types::DatabricksLoadTestCase;

const STAGED: &str = "/Volumes/main/raw/stage/sqlload/";
const COPY_ORDERS: &str = "COPY INTO `main`.`raw`.`orders` FROM (SELECT ";

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
            description: "a failed run's folder over a day old is removed; a load still writing keeps its folder",
            prepare: stale_and_running_loads,
            expected_rows: Some(6),
            expected_left: &[
                "/Volumes/main/raw/stage/sqlload/running-load",
                "/Volumes/main/raw/stage/sqlload/running-load/00001",
                "/Volumes/main/raw/stage/sqlload/running-load/00001/part-00001.parquet",
            ],
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
