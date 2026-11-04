//! Move data for every unblocked table of a plan.

use std::collections::BTreeMap;

use crate::loader::_helpers::clock::now_utc_text;
use crate::loader::_helpers::destination::{execute, finish_staging, stage_batch};
use crate::loader::_helpers::sources::connect_source;
use crate::loader::constants::SOURCE_BATCH_ROWS;
use crate::loader::errors::LoaderError;
use crate::loader::models::{Destination, LoadResult, ProjectPlan, StagingSession, TablePlan};
use crate::postgres_source::main::copy_table::copy_table;
use crate::postgres_source::models::CopyRequest;
use crate::project::models::ProjectConfig;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};
use crate::write_sql::main::display_name::display_name;
use crate::write_sql::main::qualified_name::qualified_name;
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
        let client = connect_source(project, source).await?;
        let rows = load_table(&client, table, destination).await?;
        results.push(LoadResult {
            table: display_name(&table.spec.destination, table.spec.warehouse),
            rows,
        });
    }
    Ok(results)
}

async fn load_table(
    client: &tokio_postgres::Client,
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
    let request = CopyRequest {
        table: &spec.source_table,
        columns: table.read_columns.clone(),
        filter: table.source_filter.clone(),
        batch_rows: SOURCE_BATCH_ROWS,
    };
    let mut session = StagingSession {
        staging: staging.clone(),
        load_id: load_id.clone(),
        parts: 0,
    };
    let copied = copy_table(client, request, async |batch| {
        session.parts += 1;
        stage_batch(destination, &session, batch)
            .await
            .map_err(|error| error.to_string())
    })
    .await;
    let staged = match copied {
        Ok(rows) => finish_staging(destination, &session).await.map(|()| rows),
        Err(error) => Err(error.into()),
    };
    let rows = match staged {
        Ok(rows) => rows,
        Err(error) => {
            let drop = format!(
                "DROP TABLE IF EXISTS {}",
                qualified_name(&staging, spec.warehouse)
            );
            let _ = execute(destination, vec![drop]).await;
            return Err(error);
        }
    };
    let statements = write_statements(
        &WriteStatementsRequest {
            warehouse: spec.warehouse,
            table: &spec.destination,
            columns: &table.schema.write_columns,
            mode: &table.mode,
            load_id: &load_id,
            loaded_at: &loaded_at,
        },
        &staging,
    );
    execute(destination, statements).await?;
    Ok(rows)
}
