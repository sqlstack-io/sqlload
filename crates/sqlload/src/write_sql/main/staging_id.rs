//! What tells one load's staging tables apart.

use chrono::NaiveDateTime;

use crate::write_sql::constants::STAGING_TIME_FORMAT;

/// The time the load started and the start of its id: the time lets a later load tell a crashed load's staging table is old, on warehouses that keep no creation time.
pub fn staging_id(load_id: &str, now: NaiveDateTime) -> String {
    let short_id: String = load_id
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .take(8)
        .collect();
    format!("{}_{short_id}", now.format(STAGING_TIME_FORMAT))
}
