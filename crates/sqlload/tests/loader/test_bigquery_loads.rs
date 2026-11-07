use crate::bigquery_helpers::{
    live_bigquery, live_bigquery_client, refused_live_load, reset_live_bigquery,
    stream_into_live_table,
};
use crate::helpers::run_async;
use crate::test_types::{LiveRefusedLoadTestCase, LiveWriteApiTestCase};

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
        sqlload::bigquery_api::main::run_query::run_query(
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

/// Skipped when `SQLLOAD_BIGQUERY_DATASET` is unset. BigQuery takes no rows yet, so `load` must refuse right after planning, before it applies or loads anything: no dataset is created.
#[test]
fn given_a_live_bigquery_target_when_loading_then_it_is_refused_before_anything_is_applied()
-> Result<(), String> {
    let Some(live) = live_bigquery()? else {
        return Ok(());
    };
    let test_cases = [LiveRefusedLoadTestCase {
        description: "load into a BigQuery target refuses at plan, and the target's dataset is never created",
        expected_succeeded: false,
        expected_refusal: "loading rows into BigQuery (target dev) is not available yet, so nothing was applied or loaded.",
        expected_datasets: "0",
    }];
    run_async(async {
        let client = live_bigquery_client(&live)?;
        for test_case in test_cases {
            let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
            let (succeeded, output, datasets) =
                refused_live_load(&client, &live, directory.path()).await?;
            assert_eq!(
                (
                    succeeded,
                    output.contains(test_case.expected_refusal),
                    datasets.as_str()
                ),
                (
                    test_case.expected_succeeded,
                    true,
                    test_case.expected_datasets
                ),
                "{}: {output}",
                test_case.description
            );
        }
        Ok(())
    })
}
