use sqlload::bigquery_api::main::run_query::run_query;

use crate::bigquery_helpers::{
    append_refused, commit_refused, live_bigquery, live_bigquery_client, nothing_in_bigquery,
    orders_without_score, reset_live_bigquery, run_bigquery, run_live_bigquery, stale_staging,
    stream_into_live_table, token_expired,
};
use crate::helpers::{found, not_found, run_async, two_batches, two_days};
use crate::test_types::{BigQueryLoadTestCase, LiveBigQueryLoadTestCase, LiveWriteApiTestCase};

const LIVE_APPEND: LiveBigQueryLoadTestCase = LiveBigQueryLoadTestCase {
    description: "",
    source: "orders_append",
    settings: "write_strategy = \"append\"",
    file_set: two_days,
    expected_rows: "6",
};

const STAGED: &str = "raw.orders__sqlload_staging_";

const BASE: BigQueryLoadTestCase = BigQueryLoadTestCase {
    description: "",
    settings: "write_strategy = \"append\"",
    file_set: two_days,
    prepare: nothing_in_bigquery,
    expected_error: "",
    expected_sent: &[],
    expected_unsent: &[],
    expected_rows: Some(6),
    expected_commits: &[STAGED],
};

#[test]
fn given_a_bigquery_target_when_loading_files_then_rows_stream_into_staging_and_commit_once_per_batch()
-> Result<(), String> {
    let test_cases = [
        BigQueryLoadTestCase {
            description: "append: rows stream into a staging table named for the load, never the table, then one INSERT moves them and the staging table is dropped",
            expected_sent: &[
                "INSERT INTO `raw`.`orders` (",
                "DROP TABLE IF EXISTS `raw`.`orders__sqlload_staging_",
            ],
            expected_unsent: &["BEGIN", "COMMIT"],
            ..BASE
        },
        BigQueryLoadTestCase {
            description: "append over two batches: one Storage Write API commit per batch",
            file_set: two_batches,
            expected_rows: Some(2002),
            expected_commits: &[STAGED, STAGED],
            ..BASE
        },
        BigQueryLoadTestCase {
            description: "replace: rows stream into a build table, which replaces the table in one statement, its description set again",
            settings: "write_strategy = \"replace\"",
            expected_sent: &[
                "CREATE OR REPLACE TABLE `raw`.`orders` AS SELECT * FROM `raw`.`orders__sqlload_staging_",
                "ALTER TABLE `raw`.`orders` SET OPTIONS (description = 'Test table.')",
            ],
            ..BASE
        },
        BigQueryLoadTestCase {
            description: "replace ignoring a column the table lacks: the build table leaves it out, and so do the rows streamed into it",
            settings: "write_strategy = \"replace\"\non_schema_change = \"ignore\"",
            prepare: orders_without_score,
            ..BASE
        },
        BigQueryLoadTestCase {
            description: "delete_insert: one MERGE inserting every staged row and deleting the window",
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"day\"",
            expected_sent: &["ON FALSE WHEN NOT MATCHED THEN INSERT"],
            expected_unsent: &["INSERT INTO `raw`.`orders` ("],
            expected_rows: None,
            ..BASE
        },
        BigQueryLoadTestCase {
            description: "an append BigQuery refuses a row of fails the load with its message, commits nothing and drops the staging table",
            prepare: append_refused,
            expected_error: "Field id is required but has no value",
            expected_rows: Some(0),
            expected_commits: &[],
            ..BASE
        },
        BigQueryLoadTestCase {
            description: "a commit BigQuery refuses fails the load naming the stream, and drops the staging table",
            prepare: commit_refused,
            expected_error: "BigQuery committed none of the streams",
            expected_rows: Some(0),
            expected_commits: &[],
            ..BASE
        },
        BigQueryLoadTestCase {
            description: "a refused token is fetched again once and the load goes on",
            prepare: token_expired,
            ..BASE
        },
        BigQueryLoadTestCase {
            description: "a staging table a failed run left long ago is dropped",
            prepare: stale_staging,
            expected_sent: &[
                "DROP TABLE IF EXISTS `raw`.`orders__sqlload_staging_20260101000000_deadbeef`",
            ],
            ..BASE
        },
    ];
    run_async(async {
        for test_case in &test_cases {
            let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
            let outcome = run_bigquery(directory.path(), test_case).await?;
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
                (
                    not_found(&outcome.sent, test_case.expected_sent),
                    found(&outcome.sent, test_case.expected_unsent),
                    test_case.expected_rows.unwrap_or(outcome.rows),
                    outcome.commits.clone(),
                    outcome.staged.clone(),
                    outcome.unrouted_calls,
                ),
                (
                    Vec::<&str>::new(),
                    Vec::<&str>::new(),
                    outcome.rows,
                    test_case
                        .expected_commits
                        .iter()
                        .map(|table| (*table).to_owned())
                        .collect::<Vec<String>>(),
                    Vec::<String>::new(),
                    0,
                ),
                "{}: not sent, sent anyway, rows, commits, staging left, unrouted calls\n{:#?}",
                test_case.description,
                outcome.sent
            );
        }
        Ok(())
    })
}

/// Skipped when `SQLLOAD_BIGQUERY_DATASET` is unset, and fails when it names a dataset outside `sqlload_`; the dataset is dropped first, streamed into, and dropped at the end, its tables expiring after a day if a failed run leaves them. It round-trips the hand-written Storage Write API messages against the real service: requests BigQuery must parse, and answers (results, error codes, row counts, stream errors) sqlload must read.
#[test]
fn given_a_live_bigquery_dataset_when_streaming_arrow_rows_then_they_appear_only_once_committed()
-> Result<(), String> {
    let Some(live) = live_bigquery()? else {
        return Ok(());
    };
    let test_cases = [
        LiveWriteApiTestCase {
            description: "two appends on one pending stream stay invisible until the commit, then appear together with their decimals, times and NULLs",
            table: "streamed",
            batches: &[&[1, 2], &[3]],
            first_offset: 0,
            finalize: true,
            expected_stream_type: 2,
            expected_append_answers: &[(0, 0, 0, -1), (0, 2, 0, -1)],
            expected_finalized_rows: 3,
            expected_visible_before_commit: "",
            expected_commit: "committed true",
            expected_rows: "1:n1:1.5:1970-01-01T00:00:01Z:odd,2:n2:3:1970-01-01T00:00:02Z:-,3:n3:4.5:1970-01-01T00:00:03Z:odd",
        },
        LiveWriteApiTestCase {
            description: "an append past the stream's end is refused as OUT_OF_RANGE and adds no rows",
            table: "past_end",
            batches: &[&[1]],
            first_offset: 5,
            finalize: true,
            expected_stream_type: 2,
            expected_append_answers: &[(11, -1, 0, -1)],
            expected_finalized_rows: 0,
            expected_visible_before_commit: "",
            expected_commit: "committed true",
            expected_rows: "",
        },
        LiveWriteApiTestCase {
            description: "a stream committed before it is finalized commits nothing, and the error names the stream and why",
            table: "unfinalized",
            batches: &[&[1]],
            first_offset: 0,
            finalize: false,
            expected_stream_type: 2,
            expected_append_answers: &[(0, 0, 0, -1)],
            expected_finalized_rows: 0,
            expected_visible_before_commit: "",
            expected_commit: "HTTP 400 streamErrors: BigQuery committed none of the streams: <stream>: Stream is not finalized. (code 5)",
            expected_rows: "",
        },
        LiveWriteApiTestCase {
            description: "a value longer than its STRING(3) column is not checked by the Write API: it commits as sent",
            table: "too_long",
            batches: &[&[7, 1000]],
            first_offset: 0,
            finalize: true,
            expected_stream_type: 2,
            expected_append_answers: &[(0, 0, 0, -1)],
            expected_finalized_rows: 2,
            expected_visible_before_commit: "",
            expected_commit: "committed true",
            expected_rows: "7:n7:10.5:1970-01-01T00:00:07Z:odd,1000:n1000:1500:1970-01-01T00:16:40Z:-",
        },
        LiveWriteApiTestCase {
            description: "a NULL in a NOT NULL column refuses the whole request as INVALID_ARGUMENT, naming the row by index",
            table: "null_id",
            batches: &[&[8, -1]],
            first_offset: 0,
            finalize: true,
            expected_stream_type: 2,
            expected_append_answers: &[(3, -1, 1, 1)],
            expected_finalized_rows: 0,
            expected_visible_before_commit: "",
            expected_commit: "committed true",
            expected_rows: "",
        },
    ];
    run_async(async {
        let client = live_bigquery_client(&live)?;
        reset_live_bigquery(&client, &live).await?;
        let run = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_millis()
            .to_string();
        for test_case in &test_cases {
            let outcome = stream_into_live_table(&client, &live, test_case, &run).await?;
            assert_eq!(
                (
                    outcome.stream_type,
                    outcome.append_answers.as_slice(),
                    outcome.finalized_rows,
                    outcome.visible_before_commit.as_str(),
                    outcome.commit.as_str(),
                    outcome.rows.as_str(),
                ),
                (
                    test_case.expected_stream_type,
                    test_case.expected_append_answers,
                    test_case.expected_finalized_rows,
                    test_case.expected_visible_before_commit,
                    test_case.expected_commit,
                    test_case.expected_rows,
                ),
                "{}",
                test_case.description
            );
        }
        run_query(
            &client,
            &format!(
                "DROP SCHEMA IF EXISTS `{}`.`{}` CASCADE",
                live.project, live.dataset
            ),
        )
        .await
        .map_err(|error| error.to_string())?;
        Ok(())
    })
}

/// Skipped when `SQLLOAD_BIGQUERY_DATASET` is unset; loads into `<dataset>_load`, dropped first and at the end.
#[test]
fn given_a_live_bigquery_dataset_when_loading_each_strategy_then_rows_land_and_nothing_stays_staged()
-> Result<(), String> {
    let Some(live) = live_bigquery()? else {
        return Ok(());
    };
    let test_cases = [
        LiveBigQueryLoadTestCase {
            description: "append",
            ..LIVE_APPEND
        },
        LiveBigQueryLoadTestCase {
            description: "append again: the files are loaded, so nothing new",
            ..LIVE_APPEND
        },
        LiveBigQueryLoadTestCase {
            description: "append over two batches: two commits of streamed rows",
            source: "orders_batches",
            file_set: two_batches,
            expected_rows: "2002",
            ..LIVE_APPEND
        },
        LiveBigQueryLoadTestCase {
            description: "replace",
            source: "orders_replace",
            settings: "write_strategy = \"replace\"",
            ..LIVE_APPEND
        },
        LiveBigQueryLoadTestCase {
            description: "replace again: the same rows, not twice, though the table was just recreated",
            source: "orders_replace",
            settings: "write_strategy = \"replace\"",
            ..LIVE_APPEND
        },
        LiveBigQueryLoadTestCase {
            description: "replace without a column the files carry",
            source: "orders_ignored",
            settings: "write_strategy = \"replace\"\nexclude_columns = [\"score\"]",
            ..LIVE_APPEND
        },
        LiveBigQueryLoadTestCase {
            description: "replace again ignoring the new column: the build table leaves it out, so its rows stream without it",
            source: "orders_ignored",
            settings: "write_strategy = \"replace\"\non_schema_change = \"ignore\"",
            ..LIVE_APPEND
        },
        LiveBigQueryLoadTestCase {
            description: "delete_insert on the day partition",
            source: "orders_days",
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"day\"",
            ..LIVE_APPEND
        },
        LiveBigQueryLoadTestCase {
            description: "delete_insert again: the last day is read again and its MERGE deletes the rows it reinserts, so they are not doubled",
            source: "orders_days",
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"day\"",
            ..LIVE_APPEND
        },
    ];
    run_async(async {
        let client = live_bigquery_client(&live)?;
        let dataset = format!("`{}`.`{}_load`", live.project, live.dataset);
        run_query(&client, &format!("DROP SCHEMA IF EXISTS {dataset} CASCADE"))
            .await
            .map_err(|error| error.to_string())?;
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        for test_case in &test_cases {
            let outcome = run_live_bigquery(directory.path(), &live, test_case).await?;
            assert_eq!(
                outcome,
                (
                    String::new(),
                    Some(test_case.expected_rows.to_owned()),
                    Some("0".to_owned())
                ),
                "{}: the error, the rows and the staging tables left",
                test_case.description
            );
        }
        run_query(&client, &format!("DROP SCHEMA IF EXISTS {dataset} CASCADE"))
            .await
            .map_err(|error| error.to_string())?;
        Ok(())
    })
}
