//! Whether a stage route sends rows without files.

use crate::adapters::snowflake::models::StageRoute;

/// Whether `route` sends rows without files (Snowpipe Streaming or batched `INSERT`), into a staging table that holds them as text.
pub(in crate::adapters) fn sends_rows(route: Option<&StageRoute>) -> bool {
    matches!(
        route,
        Some(StageRoute::Streaming { .. } | StageRoute::Insert)
    )
}
