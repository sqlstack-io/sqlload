//! Whether another load holds a files table now.

use chrono::{DateTime, Utc};

use crate::file_store::models::OpenFolder;
use crate::table_lease::_helpers::newest::newest_lease;
use crate::table_lease::_helpers::text::lease_refusal;
use crate::table_lease::errors::LeaseError;

/// Why the table in `folder` cannot be loaded at `now`, while a load holds an unexpired lease on it; `None` when it is free.
pub async fn lease_holder(
    folder: &OpenFolder,
    now: DateTime<Utc>,
) -> Result<Option<String>, LeaseError> {
    let (_, newest) = newest_lease(folder).await?;
    Ok(newest
        .filter(|lease| lease.expires_at > now)
        .map(|lease| lease_refusal(&lease)))
}
