//! Check that MotherDuck can read a files source's files on its servers.

use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::main::_pull_files::pull_files;
use crate::adapters::duckdb::models::{DuckDbDestination, DuckDbSql};
use crate::store_paths::models::StoreKind;

/// Check that MotherDuck reads `url`, a Parquet file in `store`, on its servers, with the session's secret for the bucket as a load sets it; nothing for SQLLoad's own DuckDB, which reads with the same keys as SQLLoad's reader.
pub(in crate::adapters) async fn check_files(
    destination: &DuckDbDestination,
    store: &StoreKind,
    url: &str,
) -> Result<(), DuckDbError> {
    if !destination.motherduck {
        return Ok(());
    }
    let probe = format!(
        "SELECT count(*) FROM read_parquet({}, MD_RUN = REMOTE)",
        DuckDbSql.render_loader_value_literal(url)
    );
    pull_files(destination, store, probe).await.map(|_| ())
}
