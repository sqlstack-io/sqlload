//! Choose how a warehouse source's table leaves its warehouse.

use crate::bigquery_source::constants::BIGQUERY_SOURCE;
use crate::databricks_source::constants::DATABRICKS_SOURCE;
use crate::duckdb_source::constants::{DUCKDB_SOURCE, MOTHERDUCK_SOURCE};
use crate::loader::models::{WarehouseRoute, WarehouseSource};
use crate::loader::constants::{ODBC_SOURCE, SNOWFLAKE_SOURCE};
use crate::project::models::{SourceConfig, SourceKind, UnloadStage};
use crate::routing::models::Store;
use crate::snowflake_source::constants::USER_STAGE_FOLDER;
use crate::store_paths::models::Location;

/// How a warehouse source's table is read, with `sql` the query behind it: a `snowflake` source unloads to its `stage`, or to the user stage when it sets none; a `bigquery` source with `export` exports to that folder (`export`), unless its target writes through the same connection and selects the rows itself; every other warehouse source is queried. `same_connection`: the target writes through the source's own connection, so on BigQuery, and on Snowflake when the source sets no `warehouse` of its own, the target may select the rows itself. `None` for a source that is no warehouse.
pub(crate) fn warehouse_source(
    (source, export): (&SourceConfig, Option<&Location>),
    sql: Option<String>,
    same_connection: bool,
) -> Option<WarehouseSource> {
    let system = match source.kind {
        SourceKind::Snowflake => SNOWFLAKE_SOURCE,
        SourceKind::BigQuery => BIGQUERY_SOURCE,
        SourceKind::Odbc => ODBC_SOURCE,
        SourceKind::Databricks => DATABRICKS_SOURCE,
        SourceKind::DuckDb => DUCKDB_SOURCE,
        SourceKind::MotherDuck => MOTHERDUCK_SOURCE,
        _ => return None,
    };
    let route = match (source.kind, export) {
        (SourceKind::Snowflake, _) => snowflake_unload(source, same_connection),
        (SourceKind::BigQuery, Some(folder)) if !same_connection => WarehouseRoute::Unload {
            store: Store::ObjectStore(folder.to_string()),
            stage: format!(
                "export: BigQuery writes Parquet to {folder} with EXPORT DATA; the files are read there and removed after the read"
            ),
            same_connection,
        },
        (kind, _) => WarehouseRoute::Query {
            same_connection: kind == SourceKind::BigQuery && same_connection,
        },
    };
    Some(WarehouseSource { system, sql, route })
}

/// Where a `snowflake` source unloads, and why there.
fn snowflake_unload(source: &SourceConfig, same_connection: bool) -> WarehouseRoute {
    let (store, stage) = match &source.stage {
        Some(UnloadStage {
            url: Some(url),
            storage_integration: Some(integration),
            ..
        }) => (
            Store::ObjectStore(format!("{}/", url.trim_end_matches('/'))),
            format!(
                "external: Snowflake writes to {url} through storage integration {integration}; the files are read there and removed after the read"
            ),
        ),
        Some(UnloadStage {
            name: Some(name), ..
        }) => (
            Store::ObjectStore(format!("@{name}")),
            format!(
                "stage {name}: Snowflake writes the files there, and SQLLoad reads them and removes them after the read"
            ),
        ),
        _ => (
            Store::InternalStage(USER_STAGE_FOLDER.to_owned()),
            "internal: no stage is set, so Snowflake writes to your user stage, and SQLLoad fetches the files in parallel, decrypts them and removes them after the read".to_owned(),
        ),
    };
    WarehouseRoute::Unload {
        store,
        stage,
        same_connection: same_connection && source.warehouse.is_none(),
    }
}
