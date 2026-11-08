//! The check behind `debug` for a Postgres target.

use crate::adapters::postgres::_helpers::session::{query_error, session};
use crate::adapters::postgres::constants::SESSION_QUERY;
use crate::adapters::postgres::models::PostgresDestination;
use crate::loader::errors::LoaderError;

/// Who the target's session connects as, to which database and server, and what protects it.
pub(in crate::adapters) async fn check_session(
    destination: &PostgresDestination,
) -> Result<String, LoaderError> {
    let held = session(destination).await?;
    let Some((client, encryption)) = held.as_ref() else {
        return Ok(String::new());
    };
    let row = client
        .query_one(SESSION_QUERY, &[])
        .await
        .map_err(query_error)?;
    let answer: String = row.try_get(0).map_err(query_error)?;
    Ok(format!("{answer}, {encryption}"))
}
