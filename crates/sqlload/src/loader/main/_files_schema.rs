//! Plan a files table's schema against its newest files.

use std::collections::BTreeSet;

use crate::adapters::files::main::fit_live_types::fit_live_types;
use crate::loader::_helpers::table_plan::blocked_plan;
use crate::loader::models::{PlanOptions, TableSpec};
use crate::schema_plan::main::plan_schema::plan_schema;
use crate::schema_plan::models::{DesiredColumn, LiveColumn, SchemaPlan, SchemaRules, TableAction};

/// The schema plan against the newest files: new columns follow `on_schema_change`, removed ones warn, type changes block.
pub(in crate::loader) fn files_schema(
    spec: &TableSpec,
    desired: &[DesiredColumn],
    live: Option<&[LiveColumn]>,
    options: &PlanOptions,
) -> SchemaPlan {
    let rules = SchemaRules {
        warehouse: spec.warehouse,
        contract: spec.contract,
        on_schema_change: spec.on_schema_change,
        auto_widen: spec.auto_widen,
        type_change_rebuild: spec.type_change_rebuild,
        rebuild_allowed: options.rebuild_allowed,
    };
    let compared = live.filter(|_| !options.reload);
    let mut schema = plan_schema(desired, compared, rules);
    schema.write_columns = fit_live_types(schema.write_columns, compared.unwrap_or_default());
    let wanted: BTreeSet<String> = desired
        .iter()
        .map(|column| column.name.to_ascii_lowercase())
        .collect();
    schema.warnings.extend(
        compared
            .unwrap_or_default()
            .iter()
            .filter(|column| !wanted.contains(&column.name.to_ascii_lowercase()))
            .map(|column| {
                format!(
                    "{} is in the existing files but not in the source; new files will not have it",
                    column.name
                )
            }),
    );
    if options.reload && live.is_some() {
        schema.action = TableAction::Recreate;
    }
    let cursor_name = spec.cursor.as_ref().map(|cursor| cursor.column.as_str());
    let written: BTreeSet<String> = schema
        .write_columns
        .iter()
        .map(|column| column.name.to_ascii_lowercase())
        .collect();
    match (schema.blocked.is_none(), cursor_name) {
        (true, Some(name))
            if spec.python.is_some()
                && !written.contains(&name.to_ascii_lowercase())
                && live.is_none() =>
        {
            blocked_plan(vec![format!(
                "{name} is the cursor_column of a Python table, but files targets need its type before the first load: declare it, for example columns={{\"{name}\": \"date\"}}"
            )])
        }
        (true, Some(name)) if !written.contains(&name.to_ascii_lowercase()) => {
            blocked_plan(vec![format!(
                "cursor_column {name} is not written to the files (excluded, not in the enforced contract, or ignored by on_schema_change)"
            )])
        }
        _ => schema,
    }
}
