//! The plan of a merge table's key scan, run after its merge.

use crate::adapters::contract::models::{MarkWrite, WriteMode};
use crate::load_naming::models::LoadedNames;
use crate::loader::models::{CommitRule, TablePlan, WholeReason};
use crate::project::models::Deletes;
use crate::schema_plan::main::metadata_columns::metadata_columns;
use crate::schema_plan::models::{DesiredColumn, SchemaPlan, TableAction};

/// The key scan `planned`'s load runs after its merge under `--sync-deletes`: every row of its source read whole (no cursor), only its `unique_key` columns, staged with the load's metadata and committed once, marking the table's rows loaded before `before` (when the read starts, RFC 3339 UTC) whose key it didn't read. `None` when the table scans no keys, or its plan is blocked.
pub fn scan_plan(planned: &TablePlan, before: &str) -> Option<TablePlan> {
    let deletes = planned
        .spec
        .deletes
        .as_ref()
        .filter(|deletes| deletes.scan && planned.schema.blocked.is_none())?;
    let WriteMode::Merge {
        unique_key,
        not_null_keys,
        ..
    } = &planned.mode
    else {
        return None;
    };
    let keyed = |name: &str| unique_key.iter().any(|key| key == name);
    let mark = MarkWrite {
        unique_key: unique_key.clone(),
        not_null_keys: not_null_keys.clone(),
        soft: deletes.apply == Deletes::Soft,
        before: before.to_owned(),
        max_drop: deletes.max_drop,
    };
    let mut spec = planned.spec.clone();
    (spec.cursor, spec.batch, spec.source_end) = (None, None, None);
    (spec.checks, spec.deletes, spec.transform) = (None, None, None);
    (spec.logged, spec.row_id, spec.constants) = (false, false, Vec::new());
    spec.names = LoadedNames::default();
    let write_columns: Vec<DesiredColumn> = planned
        .schema
        .write_columns
        .iter()
        .filter(|column| keyed(&column.name))
        .cloned()
        .chain(metadata_columns(spec.compat, (false, false)))
        .collect();
    Some(TablePlan {
        read_columns: planned
            .read_columns
            .iter()
            .filter(|column| keyed(&column.name))
            .cloned()
            .collect(),
        live: planned.live.clone(),
        schema: SchemaPlan {
            action: TableAction::Unchanged,
            write_columns,
            warnings: Vec::new(),
            blocked: None,
        },
        mode: WriteMode::MarkMissing(mark),
        source_filter: None,
        source_parameters: Vec::new(),
        cursor_note: None,
        window: None,
        route: planned.route.clone(),
        files: None,
        output: None,
        exists: true,
        logged: None,
        commits: CommitRule::Whole(WholeReason::KeyScan),
        stage: planned.stage.clone(),
        planned_stage: None,
        fetch_memory: planned.fetch_memory.clone(),
        spec,
    })
}
