//! A warehouse's SQL adapter.

use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::databricks::models::DatabricksSql;
use crate::adapters::duckdb::models::DuckDbSql;
use crate::adapters::files::models::ParquetFiles;
use crate::adapters::snowflake::models::SnowflakeSql;
use crate::column_types::models::Warehouse;

/// The adapter rendering `warehouse`'s SQL and types.
pub fn sql_adapter(warehouse: Warehouse) -> &'static dyn SqlAdapter {
    match warehouse {
        Warehouse::Snowflake => &SnowflakeSql,
        Warehouse::DuckDb => &DuckDbSql,
        Warehouse::Parquet => &ParquetFiles,
        Warehouse::Databricks => &DatabricksSql,
    }
}
