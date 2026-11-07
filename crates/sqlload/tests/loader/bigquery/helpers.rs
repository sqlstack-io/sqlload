//! Helpers for the BigQuery loader tests: the live project and the rows streamed into it.

use std::sync::Arc;

use arrow_array::{
    Decimal128Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use sqlload::bigquery_api::main::append_rows::append_rows;
use sqlload::bigquery_api::main::arrow_request::arrow_request;
use sqlload::bigquery_api::main::batch_commit_write_streams::batch_commit_write_streams;
use sqlload::bigquery_api::main::create_write_stream::create_write_stream;
use sqlload::bigquery_api::main::finalize_write_stream::finalize_write_stream;
use sqlload::bigquery_api::main::parse_key::parse_key;
use sqlload::bigquery_api::main::run_query::run_query;
use sqlload::bigquery_api::main::table_path::table_path;
use sqlload::bigquery_api::models::{AppendRowsRequest, AppendStream, BigQueryClient};

use crate::test_types::{LiveBigQuery, LiveWriteApiTestCase, WriteApiOutcome};

/// The key's project and the dataset the live BigQuery tests use; `Ok(None)` when `SQLLOAD_BIGQUERY_DATASET` is unset, which skips them.
pub(crate) fn live_bigquery() -> Result<Option<LiveBigQuery>, String> {
    let Ok(dataset) = std::env::var("SQLLOAD_BIGQUERY_DATASET") else {
        return Ok(None);
    };
    dataset.starts_with("sqlload_").then_some(()).ok_or_else(|| format!(
        "SQLLOAD_BIGQUERY_DATASET must start with sqlload_, the only datasets this test may create and drop, not {dataset:?}"
    ))?;
    let key = std::env::var("GCP_BQ_SANDBOX_SA_KEY")
        .map_err(|_| "GCP_BQ_SANDBOX_SA_KEY is not set".to_owned())?;
    let project = parse_key(&key)
        .map_err(|error| error.to_string())?
        .project_id;
    Ok(Some(LiveBigQuery {
        key,
        project,
        location: std::env::var("SQLLOAD_BIGQUERY_LOCATION")
            .unwrap_or_else(|_| "europe-west2".to_owned()),
        dataset,
    }))
}

/// A client for the live project.
pub(crate) fn live_bigquery_client(live: &LiveBigQuery) -> Result<BigQueryClient, String> {
    Ok(BigQueryClient::new(
        sqlload::http::main::http_client::build_http_client(
            &sqlload::http::models::ConnectionCounter::default(),
        )
        .map_err(|error| error.to_string())?,
        (&live.project, &live.location),
        parse_key(&live.key).map_err(|error| error.to_string())?,
        (None, None),
    ))
}

/// Drop the live dataset, then create it again empty in the live location.
pub(crate) async fn reset_live_bigquery(
    client: &BigQueryClient,
    live: &LiveBigQuery,
) -> Result<(), String> {
    let dataset = format!("`{}`.`{}`", live.project, live.dataset);
    for statement in [
        format!("DROP SCHEMA IF EXISTS {dataset} CASCADE"),
        format!(
            "CREATE SCHEMA {dataset} OPTIONS (location = '{}')",
            live.location
        ),
    ] {
        run_query(client, &statement)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

/// `ids` as orders: `name` `n<id>`, `amount` 1.50 × id, `placed_at` id seconds after the epoch, `note` `odd` or NULL.
fn orders(ids: &[i64]) -> Result<RecordBatch, String> {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Decimal128(12, 2), true),
        Field::new(
            "placed_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("note", DataType::Utf8, true),
    ]);
    let amounts = Decimal128Array::from(
        ids.iter()
            .map(|id| i128::from(*id) * 150)
            .collect::<Vec<i128>>(),
    )
    .with_precision_and_scale(12, 2)
    .map_err(|error| error.to_string())?;
    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|id| Some(format!("n{id}")))
                    .collect::<Vec<Option<String>>>(),
            )),
            Arc::new(amounts),
            Arc::new(
                TimestampMicrosecondArray::from(
                    ids.iter().map(|id| id * 1_000_000).collect::<Vec<i64>>(),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|id| (id % 2 == 1).then_some("odd"))
                    .collect::<Vec<Option<&str>>>(),
            )),
        ],
    )
    .map_err(|error| error.to_string())
}

/// The table's rows as `id:name:amount:placed_at:note`, by id; empty when it has none.
async fn table_rows(client: &BigQueryClient, table: &str) -> Result<String, String> {
    let rows = run_query(
        client,
        &format!("SELECT STRING_AGG(FORMAT('%d:%s:%s:%s:%s', id, name, CAST(amount AS STRING), FORMAT_TIMESTAMP('%FT%TZ', placed_at), IFNULL(note, '-')), ',' ORDER BY id) FROM {table}"),
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(rows
        .rows
        .into_iter()
        .next()
        .and_then(|row| row.into_iter().next().flatten())
        .unwrap_or_default())
}

/// The error code of the next answer on `appends`, 0 when BigQuery accepted the request, with how many rows it refused.
async fn append_answer(appends: &mut AppendStream) -> Result<(i32, usize), String> {
    let answer = appends
        .responses
        .message()
        .await
        .map_err(|status| status.to_string())?
        .ok_or_else(|| "the append stream ended early".to_owned())?;
    Ok((
        answer.error.map_or(0, |error| error.code),
        answer.row_errors.len(),
    ))
}

/// Stream the case's batches into a new table through one pending stream, finalize it when the case does, then commit it. The table is named for the case and `run`, because the Write API keeps refusing a dropped table's name for a while.
pub(crate) async fn stream_into_live_table(
    client: &BigQueryClient,
    live: &LiveBigQuery,
    test_case: &LiveWriteApiTestCase,
    run: &str,
) -> Result<WriteApiOutcome, String> {
    let name = format!("{}_{run}", test_case.table);
    let table = format!("`{}`.`{}`.`{name}`", live.project, live.dataset);
    run_query(
        client,
        &format!("CREATE TABLE {table} (id INT64 NOT NULL, name STRING, amount NUMERIC(12, 2), placed_at TIMESTAMP, note STRING)"),
    )
    .await
    .map_err(|error| error.to_string())?;
    let parent = table_path(&live.project, &live.dataset, &name);
    let stream = create_write_stream(client, &parent)
        .await
        .map_err(|error| error.to_string())?;
    let mut requests: Vec<AppendRowsRequest> = Vec::new();
    let mut offset = test_case.first_offset;
    for (index, ids) in test_case.batches.iter().enumerate() {
        requests.push(
            arrow_request(&stream.name, offset, &orders(ids)?, index == 0)
                .map_err(|error| error.to_string())?,
        );
        offset += i64::try_from(ids.len()).map_err(|error| error.to_string())?;
    }
    let mut pending = requests.into_iter();
    let first = pending
        .next()
        .ok_or_else(|| "every case appends at least one batch".to_owned())?;
    let mut appends = append_rows(client, first)
        .await
        .map_err(|error| error.to_string())?;
    let mut outcome = WriteApiOutcome {
        append_answers: vec![append_answer(&mut appends).await?],
        ..WriteApiOutcome::default()
    };
    for request in pending {
        appends
            .requests
            .send(request)
            .await
            .map_err(|error| error.to_string())?;
        outcome
            .append_answers
            .push(append_answer(&mut appends).await?);
    }
    drop(appends);
    outcome.finalized_rows = futures_util::future::OptionFuture::from(
        test_case
            .finalize
            .then(|| finalize_write_stream(client, &stream.name)),
    )
    .await
    .transpose()
    .map_err(|error| error.to_string())?
    .unwrap_or_default();
    outcome.visible_before_commit = table_rows(client, &table).await?;
    outcome.commit =
        batch_commit_write_streams(client, &parent, std::slice::from_ref(&stream.name))
            .await
            .map_or_else(
                |error| error.to_string().replace(&stream.name, "<stream>"),
                |_| "committed".to_owned(),
            );
    outcome.rows = table_rows(client, &table).await?;
    Ok(outcome)
}
