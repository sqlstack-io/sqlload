use std::collections::{BTreeMap, BTreeSet};

use crate::adapters::contract::models::WriteMode;
use crate::adapters::dispatch::main::session::clean_stage::clean_stage;
use crate::adapters::dispatch::main::session::drop_stale_staging::drop_stale_staging;
use crate::adapters::files::constants::PARQUET_SUFFIX;
use crate::adapters::files::main::newest_load_files::newest_load_files;
use crate::adapters::files::main::table_path::table_path;
use crate::adapters::files::models::{FilesDestination, TableFiles, TablePath, TablePathRequest};
use crate::column_types::models::ColumnType;
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::main::footer_columns::footer_columns;
use crate::files_source::main::read_footers::read_footers;
use crate::loader::_helpers::table_plan::blocked_plan;
use crate::loader::errors::LoaderError;
use crate::loader::models::{LiveFiles, LoadRun, ProjectPlan, TablePlan, TableSpec};
use crate::project::constants::DEFAULT_DESTINATION_PATH;
use crate::project::models::WriteStrategy;
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

/// The plan with every selected table blocked that shares its destination table with another configured table (selected or not) when either sets `constant_columns` and the load could remove or overwrite the other's rows: either uses `replace` or `delete_insert`, this run reloads it, it keeps its position in the run log (which is per destination table), or it merges on a `unique_key` without a constant column.
pub(crate) fn block_shared_destinations(
    mut plan: ProjectPlan,
    configured: &[TableSpec],
) -> ProjectPlan {
    for table in plan
        .tables
        .iter_mut()
        .filter(|table| table.schema.blocked.is_none())
    {
        let others: Vec<&TableSpec> = configured
            .iter()
            .filter(|other| shares_destination(&table.spec, other))
            .collect();
        let opted_in = !table.spec.constants.is_empty()
            || others.iter().any(|other| !other.constants.is_empty());
        if others.is_empty() || !opted_in {
            continue;
        }
        let reasons = shared_problems(table, &others);
        if !reasons.is_empty() {
            table.schema = blocked_plan(reasons);
        }
    }
    plan
}

/// Whether `other` is a different table loading into `spec`'s destination table.
fn shares_destination(spec: &TableSpec, other: &TableSpec) -> bool {
    other.target == spec.target
        && other.destination == spec.destination
        && (other.source_name != spec.source_name
            || other.source_table.qualified_name() != spec.source_table.qualified_name())
}

/// Why `table` cannot load into a destination table `others` also load.
fn shared_problems(table: &TablePlan, others: &[&TableSpec]) -> Vec<String> {
    let spec = &table.spec;
    let names: Vec<String> = others
        .iter()
        .map(|other| {
            format!(
                "{}.{}",
                other.source_name,
                other.source_table.qualified_name()
            )
        })
        .collect();
    let place = format!(
        "{} is also loaded by {}, and constant_columns tell their rows apart",
        spec.destination.table,
        names.join(", ")
    );
    let mut problems: Vec<String> = Vec::new();
    let removes = std::iter::once(spec)
        .chain(others.iter().copied())
        .find(|each| {
            matches!(
                each.strategy,
                WriteStrategy::Replace | WriteStrategy::DeleteInsert
            )
        });
    if let Some(removing) = removes {
        problems.push(format!(
            "{place}; {}.{} uses write_strategy = \"{}\", which would remove the other tables' rows: use merge on a unique_key that includes a constant column, or append",
            removing.source_name,
            removing.source_table.qualified_name(),
            removing.strategy
        ));
    }
    if table.mode == WriteMode::Recreate {
        problems.push(format!(
            "{place}; --reload would drop the other tables' rows"
        ));
    }
    if spec.logged {
        problems.push(format!(
            "{place}; state = \"logged\" keeps one position per destination table, which they would share"
        ));
    }
    let keyed = spec.unique_key.iter().any(|key| is_constant(spec, key));
    if spec.strategy == WriteStrategy::Merge && !keyed {
        problems.push(format!(
            "{place}; its unique_key ({}) has no constant column, so its merge could overwrite their rows: add one of its constant columns to it",
            spec.unique_key.join(", ")
        ));
    }
    problems
}

/// Whether `name` is one of `spec`'s constant columns, ignoring case.
fn is_constant(spec: &TableSpec, name: &str) -> bool {
    spec.constants
        .iter()
        .any(|(constant, _, _)| constant.eq_ignore_ascii_case(name))
}
