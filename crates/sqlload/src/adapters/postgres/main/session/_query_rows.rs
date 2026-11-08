//! Read a query's rows.

use tokio_postgres::SimpleQueryMessage;

use crate::adapters::postgres::_helpers::session::{query_error, session};
use crate::adapters::postgres::models::PostgresDestination;
use crate::loader::errors::LoaderError;

/// The first column of every row `query` returns, as Postgres prints it, NULL included.
pub(in crate::adapters) async fn query_rows(
    destination: &PostgresDestination,
    query: &str,
) -> Result<Vec<Option<String>>, LoaderError> {
    let held = session(destination).await?;
    let Some((client, _)) = held.as_ref() else {
        return Ok(Vec::new());
    };
    let messages = client.simple_query(query).await.map_err(query_error)?;
    Ok(messages
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row.get(0).map(str::to_owned)),
            _ => None,
        })
        .collect())
}
