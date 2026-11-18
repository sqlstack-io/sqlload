//! Name Databricks and BigQuery objects in SQL.

use crate::config::models::Config;

/// `` `catalog`.`schema`.`name` `` (Databricks) or `` `project`.`dataset`.`name` `` (BigQuery) in the destination schema; the names hold no backtick, as configuration checks.
pub fn quoted_table(config: &Config, name: &str) -> String {
    [
        config.destination.database(),
        config.destination.schema(),
        name,
    ]
    .map(|part| format!("`{}`", part.replace('`', "``")))
    .join(".")
}
