//! Pick out the staging tables crashed loads left.

use chrono::NaiveDateTime;

use crate::column_types::models::Warehouse;
use crate::adapters::contract::constants::{STAGING_SUFFIX, STAGING_TIME_DIGITS, STAGING_TIME_FORMAT};
use crate::adapters::contract::main::qualified_name::qualified_name;
use crate::adapters::contract::models::TableName;

/// `DROP` statements for the tables among `found`, in `schema_of`'s schema, that are named as staging tables and stale: on Snowflake its query already chose them by when they last changed, elsewhere the time in the name must be before `before`. Any other table, or elsewhere a name without a readable time, stays.
pub fn stale_staging(
    schema_of: &TableName,
    found: &[String],
    before: NaiveDateTime,
    warehouse: Warehouse,
) -> Vec<String> {
    found
        .iter()
        .filter(|name| match warehouse {
            Warehouse::Snowflake => name
                .to_ascii_lowercase()
                .contains(&format!("{STAGING_SUFFIX}_")),
            _ => staging_time(name).is_some_and(|time| time < before),
        })
        .map(|name| {
            let table = TableName {
                table: name.clone(),
                ..schema_of.clone()
            };
            format!("DROP TABLE IF EXISTS {}", qualified_name(&table, warehouse))
        })
        .collect()
}

/// The time in a staging table's name.
fn staging_time(name: &str) -> Option<NaiveDateTime> {
    let lower = name.to_ascii_lowercase();
    let (_, after) = lower.rsplit_once(&format!("{STAGING_SUFFIX}_"))?;
    let digits = after.get(..STAGING_TIME_DIGITS)?;
    match NaiveDateTime::parse_from_str(digits, STAGING_TIME_FORMAT) {
        Ok(time) => Some(time),
        Err(_) => None,
    }
}
