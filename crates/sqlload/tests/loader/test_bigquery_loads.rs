use crate::bigquery_helpers::{
    live_bigquery, live_bigquery_client, reset_live_bigquery, stream_into_live_table,
};
use crate::helpers::run_async;
use crate::test_types::{LiveWriteApiTestCase, WriteApiOutcome};

/// Skipped unless `SQLLOAD_BIGQUERY_DATASET` names a `sqlload_` dataset, which it drops first, streams into, and drops at the end. It round-trips the hand-written Storage Write API messages against the real service: requests BigQuery must parse, and answers (results, errors, commit times, stream errors) sqlload must read.
#[test]
fn given_a_live_bigquery_dataset_when_streaming_arrow_rows_then_they_appear_only_once_committed()
-> Result<(), String> {
    let Some(live) = live_bigquery()? else {
        return Ok(());
    };
    let test_cases = [
        LiveWriteApiTestCase {
            description: "two appends on one pending stream stay invisible until the commit, then appear together",
            table: "streamed",
            batches: &[&[1, 2], &[3]],
            first_offset: 0,
            finalize: true,
            expected: WriteApiOutcome {
                error: String::new(),
                finalized_rows: 3,
                visible_before_commit: String::new(),
                rows: "1:n1:1.5:1970-01-01T00:00:01Z:odd,2:n2:3:1970-01-01T00:00:02Z:-,3:n3:4.5:1970-01-01T00:00:03Z:odd".to_owned(),
            },
        },
        LiveWriteApiTestCase {
            description: "an append past the stream's end is refused with BigQuery's code and reason",
            table: "past_end",
            batches: &[&[1]],
            first_offset: 5,
            finalize: true,
            expected: WriteApiOutcome {
                error: "append refused: 11: ".to_owned(),
                ..WriteApiOutcome::default()
            },
        },
        LiveWriteApiTestCase {
            description: "a stream committed before it is finalized commits nothing, and says which stream",
            table: "unfinalized",
            batches: &[&[1]],
            first_offset: 0,
            finalize: false,
            expected: WriteApiOutcome {
                error: "HTTP 400 streamErrors: BigQuery committed none of the streams: projects/".to_owned(),
                ..WriteApiOutcome::default()
            },
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
            let shown = WriteApiOutcome {
                error: match outcome.error.starts_with(&test_case.expected.error)
                    && !test_case.expected.error.is_empty()
                {
                    true => test_case.expected.error.clone(),
                    false => outcome.error.clone(),
                },
                ..outcome.clone()
            };
            assert_eq!(
                shown, test_case.expected,
                "{}: {outcome:?}",
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
