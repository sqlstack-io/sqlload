//! The warehouse a destination connection writes to.

use crate::column_types::models::Warehouse;
use crate::project::models::ConnectionConfig;

/// The warehouse `connection` writes to, with its default database where it has one; `None` when it is not a destination.
pub fn connection_warehouse(connection: &ConnectionConfig) -> Option<(Warehouse, Option<String>)> {
    match connection {
        ConnectionConfig::Snowflake { database, .. } => {
            Some((Warehouse::Snowflake, Some(database.clone())))
        }
        ConnectionConfig::Databricks { catalog, .. } => {
            Some((Warehouse::Databricks, Some(catalog.clone())))
        }
        ConnectionConfig::BigQuery { project, .. } => Some((Warehouse::BigQuery, project.clone())),
        ConnectionConfig::DuckDb { .. } => Some((Warehouse::DuckDb, None)),
        ConnectionConfig::S3 { .. } | ConnectionConfig::Filesystem { .. } => {
            Some((Warehouse::Parquet, None))
        }
        ConnectionConfig::Postgres { .. } | ConnectionConfig::Http { .. } => None,
    }
}
