//! Why the destination cannot load a files table's files exactly by reading them itself.

use crate::adapters::bigquery::constants::NO_INTERNAL_STAGE_COPY as BIGQUERY_NO_INTERNAL_STAGE_COPY;
use crate::adapters::databricks::constants::NO_INTERNAL_STAGE_COPY;
#[cfg(feature = "duckdb")]
use crate::adapters::duckdb::main::_direct_problem::direct_problem;
use crate::adapters::postgres::constants::NO_INTERNAL_STAGE_COPY as POSTGRES_NO_INTERNAL_STAGE_COPY;
use crate::adapters::snowflake::main::copy::_direct_check::direct_check;
use crate::adapters::snowflake::main::copy::_stage_covers::stage_covers;
use crate::adapters::snowflake::models::StageRoute;
use crate::column_types::models::Warehouse;
use crate::files_source::models::FilesRead;
use crate::loader::models::TableSpec;

/// Why the destination cannot load the planned `read` of a files table exactly from its files: for DuckDB, by reading them itself wherever they are; for Snowflake, under the external `stage` that covers them, or copied unchanged to an internal stage (`copy_to_internal_stage`); `None` when it can, or when neither applies.
pub fn direct_blocked(
    spec: &TableSpec,
    read: &FilesRead,
    stage: Option<&StageRoute>,
) -> Option<String> {
    let files = spec.files.as_ref()?;
    let refused = match spec.warehouse {
        Warehouse::Databricks => Some(NO_INTERNAL_STAGE_COPY),
        Warehouse::BigQuery => Some(BIGQUERY_NO_INTERNAL_STAGE_COPY),
        Warehouse::Postgres => Some(POSTGRES_NO_INTERNAL_STAGE_COPY),
        _ => None,
    };
    if let Some(refused) = refused {
        return files.copy_to_internal_stage.then(|| refused.to_owned());
    }
    #[cfg(feature = "duckdb")]
    if spec.warehouse == Warehouse::DuckDb {
        return direct_problem(spec, read);
    }
    let covering = match stage {
        Some(StageRoute::External(stage)) if stage_covers(&stage.location, &files.location) => {
            Some(stage)
        }
        _ => None,
    };
    if covering.is_none() && !files.copy_to_internal_stage {
        return None;
    }
    direct_check(spec, (read, read.files.as_slice(), &[]), covering)
}
