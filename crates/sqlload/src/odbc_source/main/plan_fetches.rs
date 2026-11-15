//! How reads of an ODBC source's tables would fetch their rows on this machine, from the driver's description of their columns.

use arrow_odbc::arrow_schema_from;
use arrow_odbc::odbc_api::Connection;

use crate::odbc_source::_helpers::batches::fetch_schema;
use crate::odbc_source::_helpers::columns::{fetch_shape, fetch_size};
use crate::odbc_source::_helpers::connect::connect;
use crate::odbc_source::_helpers::memory::{budget_note, fetch_budget, memory_limit, memory_line};
use crate::odbc_source::_helpers::query::select_query;
use crate::odbc_source::errors::OdbcSourceError;
use crate::odbc_source::models::{
    FetchShape, OdbcReadRequest, OdbcReader, PlannedFetch, ValueLimits,
};

/// For each of `requests`, in order, how its rows would be fetched: under the budget of `set` (the source's `max_batch_memory`), else 512 MiB lowered to half this machine's memory limit (a container's cgroup limit, else its physical memory) shared between `concurrency` tables; in bulk or value by value, as the driver describes the prepared `SELECT` (without its filter). Nothing is run. All are described over one connection; when it can't be made, or the driver can't describe a read, its `fetch` line says why.
pub async fn plan_fetches(
    reader: &OdbcReader,
    requests: &[OdbcReadRequest<'_>],
    (set, concurrency): (Option<usize>, usize),
) -> Vec<PlannedFetch> {
    let (limit, unreadable) = match memory_limit() {
        Ok(limit) => (limit, None),
        Err(reason) => (None, Some(reason)),
    };
    let budget = fetch_budget(set, limit, concurrency);
    let note = budget_note(&budget, unreadable.as_deref());
    let queries: Vec<String> = requests
        .iter()
        .map(|request| {
            select_query(&OdbcReadRequest {
                filter: None,
                parameters: &[],
                ..*request
            })
        })
        .collect();
    let reader = reader.clone();
    let described = tokio::task::spawn_blocking(move || describe_all(&reader, &queries)).await;
    let described: Vec<Result<FetchShape, String>> = match described {
        Ok(Ok(shapes)) => shapes,
        Ok(Err(error)) => vec![Err(error.to_string()); requests.len()],
        Err(error) => vec![Err(format!("the ODBC description stopped: {error}")); requests.len()],
    };
    let mut planned: Vec<PlannedFetch> = Vec::with_capacity(requests.len());
    for (shape, request) in described.into_iter().zip(requests) {
        let (estimate, memory) = match shape {
            Ok(shape) => {
                let size = fetch_size(&shape, budget.bytes, request.batch_rows);
                (Some(size.bytes), memory_line(&shape, &size))
            }
            Err(error) => (
                None,
                format!("unknown: the driver doesn't describe the columns ({error})"),
            ),
        };
        planned.push(PlannedFetch {
            budget: budget.bytes,
            estimate,
            memory,
            budget_note: note.clone(),
            limit: budget.limit.map(|limit| limit.bytes),
        });
    }
    planned
}

/// The shape of each of `queries`' reads, over one connection.
fn describe_all(
    reader: &OdbcReader,
    queries: &[String],
) -> Result<Vec<Result<FetchShape, String>>, OdbcSourceError> {
    let connection = connect(reader)?;
    let dbms: String = connection.database_management_system_name()?;
    let mut shapes: Vec<Result<FetchShape, String>> = Vec::with_capacity(queries.len());
    for query in queries {
        shapes.push(
            described(&connection, (&dbms, query), &reader.limits)
                .map_err(|error| error.to_string()),
        );
    }
    Ok(shapes)
}

/// The shape of `query`'s read as the driver describes it prepared.
fn described(
    connection: &Connection<'_>,
    (dbms, query): (&str, &str),
    limits: &ValueLimits,
) -> Result<FetchShape, OdbcSourceError> {
    let mut prepared = connection.prepare(query)?;
    let schema = fetch_schema(&arrow_schema_from(&mut prepared, Some(dbms), false)?);
    fetch_shape(&mut prepared, &schema, limits)
}
