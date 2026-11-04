//! Move data for every unblocked table of a plan.

use std::collections::BTreeMap;

use crate::loader::_helpers::clock::now_utc_text;
use crate::loader::_helpers::destination::{cleanup_staging, execute, finish_staging, stage_batch};
use crate::loader::_helpers::sources::{open_source, read_source};
use crate::loader::errors::LoaderError;
use crate::loader::models::{
    Destination, LoadResult, ProjectPlan, SourceReader, StagingSession, TablePlan,
};
use crate::masking::main::mask_batch::mask_batch;
use crate::project::models::ProjectConfig;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};
use crate::write_sql::main::display_name::display_name;
use crate::write_sql::main::staging_table::staging_table;
use crate::write_sql::main::write_statements::write_statements;
use crate::write_sql::models::WriteStatementsRequest;

/// Load each unblocked table: read the source into a staging table, then apply the write strategy in one transaction.
pub async fn load_project(
    project: &ProjectConfig,
    plan: &ProjectPlan,
    destinations: &BTreeMap<String, Destination>,
) -> Result<Vec<LoadResult>, LoaderError> {
    let mut results: Vec<LoadResult> = Vec::new();
    for table in plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none())
    {
        let source = project
            .sources
            .get(&table.spec.source_name)
            .ok_or_else(|| {
                LoaderError::Invalid(format!(
                    "source {:?} does not exist",
                    table.spec.source_name
                ))
            })?;
        let destination = destinations.get(&table.spec.target).ok_or_else(|| {
            LoaderError::Invalid(format!("target {:?} is not connected", table.spec.target))
        })?;
        let reader = open_source(project, source).await?;
        let rows = load_table(&reader, table, destination).await?;
        results.push(LoadResult {
            table: display_name(&table.spec.destination, table.spec.warehouse),
            rows,
        });
    }
    Ok(results)
}

async fn load_table(
    reader: &SourceReader,
    table: &TablePlan,
    destination: &Destination,
) -> Result<u64, LoaderError> {
    let spec = &table.spec;
    let load_id = uuid::Uuid::new_v4().to_string();
    let loaded_at = now_utc_text();
    let data_columns: Vec<DesiredColumn> = table
        .read_columns
        .iter()
        .map(|column| DesiredColumn {
            name: column.name.clone(),
            column_type: column.output.clone(),
            origin: ColumnOrigin::Source,
        })
        .collect();
    let (staging, create) =
        staging_table(&spec.destination, &data_columns, &load_id, spec.warehouse);
    execute(destination, vec![create]).await?;
    let session = StagingSession {
        staging: staging.clone(),
        load_id: load_id.clone(),
        loaded_at,
        parts: 0,
    };
    let (session, written) = stage_and_write(reader, table, destination, session).await;
    cleanup_staging(destination, &session, spec.warehouse).await;
    written
}

/// Read the source into the staging table, then apply the write strategy in one transaction.
async fn stage_and_write(
    reader: &SourceReader,
    table: &TablePlan,
    destination: &Destination,
    mut session: StagingSession,
) -> (StagingSession, Result<u64, LoaderError>) {
    let spec = &table.spec;
    let written = async {
        let rows = read_source(reader, table, async |batch| {
            session.parts += 1;
            let batch = mask_batch(batch, &spec.masked)?;
            stage_batch(destination, &session, batch)
                .await
                .map_err(|error| error.to_string())
        })
        .await?;
        finish_staging(destination, &session).await?;
        let statements = write_statements(
            &WriteStatementsRequest {
                warehouse: spec.warehouse,
                table: &spec.destination,
                columns: &table.schema.write_columns,
                mode: &table.mode,
                load_id: &session.load_id,
                loaded_at: &session.loaded_at,
            },
            &session.staging,
        );
        execute(destination, statements).await?;
        Ok(rows)
    }
    .await;
    (session, written)
}
