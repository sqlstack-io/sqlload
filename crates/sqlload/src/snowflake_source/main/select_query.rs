//! The query an unload runs, for a destination that selects the rows itself in the same session.

use crate::snowflake_source::_helpers::unload_query::unload_query;
use crate::snowflake_source::errors::SnowflakeSourceError;
use crate::snowflake_source::models::UnloadRequest;

/// The `SELECT` an unload of `request` writes to Parquet: each read column under its own name, times and timestamps as whole microseconds, filtered as `request` says.
pub fn select_query(request: &UnloadRequest<'_>) -> Result<String, SnowflakeSourceError> {
    unload_query(request).map_err(SnowflakeSourceError::Invalid)
}
