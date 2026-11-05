use crate::http::errors::ApiError;
use crate::sql_api::models::SqlRows;

/// The rows a result loaded, summed over its per-file rows; none for a statement that is not a `COPY`.
pub(crate) fn rows_loaded(result: &SqlRows) -> Result<u64, ApiError> {
    let Some(column) = result.column("rows_loaded") else {
        return Ok(0);
    };
    let mut rows: u64 = 0;
    for row in &result.rows {
        let loaded = row.get(column).cloned().flatten().unwrap_or_default();
        rows += loaded.parse::<u64>().map_err(|error| {
            ApiError::Protocol(format!("COPY reported rows_loaded {loaded:?}: {error}"))
        })?;
    }
    Ok(rows)
}
