//! Every warehouse SQLLoad writes to.

use crate::column_types::models::Warehouse;

/// Every warehouse, so the contract tests run against each adapter; a new warehouse fails to compile here until it is listed.
pub(in crate::adapters) fn every_warehouse() -> Vec<Warehouse> {
    [Warehouse::Snowflake, Warehouse::DuckDb, Warehouse::Parquet]
        .into_iter()
        .filter(|warehouse| match warehouse {
            Warehouse::Snowflake | Warehouse::DuckDb | Warehouse::Parquet => true,
        })
        .collect()
}
