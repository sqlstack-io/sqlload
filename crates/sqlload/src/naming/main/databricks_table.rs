//! Name Databricks objects in SQL.

use crate::config::models::Config;

/// `` `catalog`.`schema`.`name` `` in the destination schema.
pub fn databricks_table(config: &Config, name: &str) -> String {
    [
        config.destination.database(),
        config.destination.schema(),
        name,
    ]
    .map(|part| format!("`{}`", part.replace('`', "``")))
    .join(".")
}
