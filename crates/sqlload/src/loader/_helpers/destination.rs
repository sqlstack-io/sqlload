use std::collections::{BTreeMap, BTreeSet};

use crate::adapters::dispatch::main::session::clean_stage::clean_stage;
use crate::adapters::dispatch::main::session::drop_stale_staging::drop_stale_staging;
use crate::adapters::files::main::newest_load_files::newest_load_files;
use crate::adapters::files::constants::PARQUET_SUFFIX;
use crate::adapters::files::main::table_path::table_path;
use crate::adapters::files::models::{FilesDestination, TableFiles, TablePath, TablePathRequest};
use crate::column_types::models::ColumnType;
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::main::footer_columns::footer_columns;
use crate::files_source::main::read_footers::read_footers;
use crate::loader::errors::LoaderError;
use crate::loader::models::{LiveFiles, LoadRun, ProjectPlan, TablePlan, TableSpec};
use crate::project::constants::DEFAULT_DESTINATION_PATH;
use crate::schema_plan::models::LiveColumn;

/// Before a load: drop the staging tables crashed loads left in the schemas it writes to. A failure only warns.
pub(crate) async fn clean_staging(run: &LoadRun<'_>, plan: &ProjectPlan) {
    let mut schemas: BTreeMap<(String, Option<String>, String), &TablePlan> = BTreeMap::new();
    for table in plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none() && table.output.is_none())
    {
        let name = &table.spec.destination;
        schemas
            .entry((
                table.spec.target.clone(),
                name.database.clone(),
                name.schema.clone(),
            ))
            .or_insert(table);
    }
    for table in schemas.into_values() {
        let Some(destination) = run.destinations.get(&table.spec.target) else {
            continue;
        };
        let name = &table.spec.destination;
        match drop_stale_staging(destination, name, table.spec.warehouse).await {
            Ok(0) => {}
            Ok(dropped) => {
                tracing::info!(dropped, schema = %name.schema, "dropped staging tables crashed loads left")
            }
            Err(error) => {
                tracing::warn!(%error, schema = %name.schema, "cannot clean up old staging tables")
            }
        }
    }
}

/// Remove the load folders failed runs left, over a day ago, on the stages this plan's tables use. A failure only warns.
pub(crate) async fn clean_stages(run: &LoadRun<'_>, plan: &ProjectPlan) {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    for table in plan
        .tables
        .iter()
        .filter(|table| table.schema.blocked.is_none())
    {
        let Some(destination) = run.destinations.get(&table.spec.target) else {
            continue;
        };
        let stage = (
            table.spec.target.as_str(),
            table.stage.as_ref(),
            &table.spec.destination,
        );
        let copies_files = table
            .spec
            .files
            .as_ref()
            .is_some_and(|files| files.copy_to_internal_stage);
        let (checked, cleaned) = clean_stage(destination, stage, copies_files, seen).await;
        seen = checked;
        for (stage, removed) in cleaned {
            match removed {
                Ok(0) => {}
                Ok(removed) => {
                    tracing::info!(removed, %stage, "removed load folders failed runs left on the stage")
                }
                Err(error) => tracing::warn!(%error, %stage, "cannot clean up the stage"),
            }
        }
    }
}

/// The table's folder and partition folder in `destination`; no I/O.
pub(crate) fn table_folder(
    spec: &TableSpec,
    destination: &FilesDestination,
) -> Result<TablePath, String> {
    table_path(&TablePathRequest {
        template: spec
            .destination_path
            .as_deref()
            .unwrap_or(DEFAULT_DESTINATION_PATH),
        connection: &destination.connection,
        store: &destination.store,
        source: &spec.source_name,
        schema: &spec.source_table.schema,
        table: &spec.source_table.name,
    })
}

/// The schema of the newest load's files in the newest partition, or why it cannot be read (too many footers, conflicting types).
pub(crate) async fn live_schema(
    folder: &OpenFolder,
    listed: &TableFiles,
    max_footer_reads: usize,
) -> Result<Result<LiveFiles, Vec<String>>, LoaderError> {
    let newest: Vec<StoredFile> = listed
        .partitions
        .last()
        .map(|partition| newest_load_files(&parquet_files(&partition.files)))
        .unwrap_or_default();
    if newest.len() > max_footer_reads {
        return Ok(Err(vec![format!(
            "the newest load in {} wrote {} files, more footers than max_footer_reads = {max_footer_reads} allows reading for its schema; raise max_footer_reads on the target",
            folder.location,
            newest.len()
        )]));
    }
    let footers = read_footers(folder, &newest).await?;
    let (columns, conflicts) = footer_columns(&footers);
    if !conflicts.is_empty() {
        return Ok(Err(conflicts));
    }
    let live: Option<Vec<LiveColumn>> = (!footers.is_empty()).then(|| {
        columns
            .into_iter()
            .map(|column| LiveColumn {
                name: column.name,
                column_type: column.column_type.unwrap_or_else(ColumnType::Native),
                fraction_digits: None,
            })
            .collect()
    });
    Ok(Ok(LiveFiles { footers, live }))
}

/// The Parquet files among `files`, whose footers hold their schema; CSV files have none.
pub(crate) fn parquet_files(files: &[StoredFile]) -> Vec<StoredFile> {
    files
        .iter()
        .filter(|file| {
            file.relative_key
                .to_ascii_lowercase()
                .ends_with(PARQUET_SUFFIX)
        })
        .cloned()
        .collect()
}
