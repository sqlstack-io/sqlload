//! Check that MotherDuck can read a files source's files on its servers, and DuckDB a GCS bucket's.

use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::main::_open_bucket::open_bucket;
use crate::adapters::duckdb::main::_pull_files::pull_files;
use crate::adapters::duckdb::models::{DuckDbDestination, DuckDbSql};
use crate::store_paths::models::StoreKind;

/// Check that MotherDuck reads `url`, a Parquet file in `store`, on its servers, or DuckDB reads it from GCS, with the session's secret for the bucket as a load sets it; nothing for SQLLoad's own DuckDB reading S3, with the same keys as SQLLoad's reader (in GCS it reads with the connection's HMAC key, which SQLLoad's reader doesn't use).
pub(in crate::adapters) async fn check_files(
    destination: &DuckDbDestination,
    store: &StoreKind,
    url: &str,
) -> Result<(), DuckDbError> {
    let gcs = matches!(store, StoreKind::Gcs { .. });
    if !destination.motherduck && !gcs {
        return Ok(());
    }
    let remote = match destination.motherduck {
        true => ", MD_RUN = REMOTE",
        false => "",
    };
    let probe = format!(
        "SELECT count(*) FROM read_parquet({}, hive_partitioning = false{remote})",
        DuckDbSql.render_loader_value_literal(url)
    );
    open_bucket(destination, store).await?;
    pull_files(destination, probe).await.map(|_| ())
}
