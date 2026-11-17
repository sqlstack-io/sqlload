//! What `plan` says about the delivery route.

use crate::config::models::{Config, Destination};
use crate::planning::models::LoadPath;

/// What `plan` says about the route beyond its objects: auto-ingest confirms loads from COPY_HISTORY, which resumes the warehouse.
pub fn load_note(config: &Config, path: LoadPath) -> Option<String> {
    let (LoadPath::S3AutoIngest, Destination::Snowflake(snowflake)) = (path, &config.destination)
    else {
        return None;
    };
    Some(format!(
        "Auto-ingest: offsets are committed once the table's COPY_HISTORY shows each file loaded. While files wait, warehouse {} resumes for one query at most once every {} (staging.pending_check_after).",
        snowflake.warehouse.as_deref().unwrap_or_default(),
        humantime::format_duration(config.staging.pending_check_after)
    ))
}
