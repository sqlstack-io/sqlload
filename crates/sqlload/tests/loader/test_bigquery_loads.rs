use crate::bigquery_helpers::{
    live_bigquery, live_bigquery_client, reset_live_bigquery, stream_into_live_table,
};
use crate::helpers::run_async;
use crate::test_types::LiveWriteApiTestCase;

/// Skipped unless `SQLLOAD_BIGQUERY_DATASET` names a `sqlload_` dataset, which it drops first, streams into, and drops at the end. It round-trips the hand-written Storage Write API messages against the real service: requests BigQuery must parse, and answers (results, error codes, row counts, stream errors) sqlload must read.
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
            expected_append_answers: &[(0, 0), (0, 0)],
            expected_finalized_rows: 3,
            expected_visible_before_commit: "",
            expected_commit: "committed",
            expected_rows: "1:n1:1.5:1970-01-01T00:00:01Z:odd,2:n2:3:1970-01-01T00:00:02Z:-,3:n3:4.5:1970-01-01T00:00:03Z:odd",
        },
        LiveWriteApiTestCase {
            description: "an append past the stream's end is refused as OUT_OF_RANGE and adds no rows",
            table: "past_end",
            batches: &[&[1]],
            first_offset: 5,
            finalize: true,
            expected_append_answers: &[(11, 0)],
            expected_finalized_rows: 0,
            expected_visible_before_commit: "",
            expected_commit: "committed",
            expected_rows: "",
        },
        LiveWriteApiTestCase {
            description: "a stream committed before it is finalized commits nothing, and the error names the stream and why",
            table: "unfinalized",
            batches: &[&[1]],
            first_offset: 0,
            finalize: false,
            expected_append_answers: &[(0, 0)],
            expected_finalized_rows: 0,
            expected_visible_before_commit: "",
            expected_commit: "HTTP 400 streamErrors: BigQuery committed none of the streams: <stream>: Stream is not finalized.",
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
                    outcome.append_answers.as_slice(),
                    outcome.finalized_rows,
                    outcome.visible_before_commit.as_str(),
                    outcome.commit.as_str(),
                    outcome.rows.as_str(),
                ),
                (
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
