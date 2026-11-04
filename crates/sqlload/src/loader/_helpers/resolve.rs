use crate::loader::_helpers::columns::ColumnSelection;
use crate::loader::_helpers::specs::{
    ColumnChoices, SourceContext, build_spec, column_selection, pinned_types,
};
use crate::loader::_helpers::targets::source_target;
use crate::loader::models::{ResolvedTables, SkippedTable, TableSpec};
use crate::postgres_source::models::SourceTable;
use crate::project::models::{ProjectConfig, TableOverride};

/// The specs for `discovered` tables of source `source_name`, with problems and skipped tables.
pub(crate) fn resolve_tables(
    project: &ProjectConfig,
    source_name: &str,
    discovered: &[SourceTable],
) -> ResolvedTables {
    let mut resolved = ResolvedTables::default();
    let Some(source) = project.sources.get(source_name) else {
        resolved
            .problems
            .push(format!("source {source_name:?} does not exist"));
        return resolved;
    };
    let target = match source_target(project, source) {
        Ok(target) => target,
        Err(problem) => {
            resolved
                .problems
                .push(format!("[sources.{source_name}]: {problem}"));
            return resolved;
        }
    };
    for table in discovered {
        let qualified = table.qualified_name();
        let listed = source.tables.get(&qualified);
        let overrides = listed.cloned().unwrap_or_default();
        let context = SourceContext {
            name: source_name,
            source,
            target: &target,
        };
        match resolve_table(&context, table, &overrides) {
            Ok(Ok(mut spec)) => {
                spec.listed = listed.is_some();
                resolved.specs.push(spec);
            }
            Ok(Err(reason)) => resolved.skipped.push(SkippedTable {
                source_name: source_name.to_owned(),
                source_table: qualified,
                reason,
            }),
            Err(problems) => resolved.problems.extend(
                problems
                    .into_iter()
                    .map(|problem| format!("{qualified}: {problem}")),
            ),
        }
    }
    for listed in source.tables.keys() {
        if !discovered
            .iter()
            .any(|table| table.qualified_name() == *listed)
        {
            resolved.problems.push(format!(
                "{listed}: listed under [sources.{source_name}.tables] but not found in the source (or not selected by include/exclude)"
            ));
        }
    }
    resolved
}

/// One table's spec, `Ok(Err(reason))` if the column rules skip it, or its problems.
fn resolve_table(
    context: &SourceContext<'_>,
    table: &SourceTable,
    overrides: &TableOverride,
) -> Result<Result<TableSpec, String>, Vec<String>> {
    let excluded =
        match column_selection(context, table, overrides).map_err(|problem| vec![problem])? {
            ColumnSelection::Skip(reason) => return Ok(Err(reason)),
            ColumnSelection::Keep(excluded) => excluded,
        };
    let pinned = pinned_types(overrides, context.target.warehouse)?;
    build_spec(
        context,
        table,
        overrides,
        ColumnChoices { excluded, pinned },
    )
    .map(Ok)
}
