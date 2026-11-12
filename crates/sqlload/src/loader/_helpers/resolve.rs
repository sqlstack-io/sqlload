use crate::adapters::dispatch::main::dialect::connection_warehouse::connection_warehouse;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::models::Warehouse;
use crate::load_naming::models::Naming;
use crate::loader::_helpers::specs::ColumnSelection;
use crate::loader::_helpers::specs::{
    ColumnChoices, SourceContext, build_spec, column_selection, pinned_types,
};
use crate::loader::models::{ResolvedTables, SkippedTable, TableSpec};
use crate::postgres_source::models::SourceTable;
use crate::project::main::motherduck_path::motherduck_path;
use crate::project::models::{
    ConnectionConfig, ProjectConfig, SourceConfig, SourceKind, TableOverride,
};
use crate::ssh_tunnel::models::SshTunnel;

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
        let overrides = source.tables.get(&qualified).cloned().unwrap_or_default();
        let context = SourceContext {
            name: source_name,
            source,
            target: &target,
        };
        match resolve_table(&context, table, &overrides) {
            Ok(Ok(mut spec)) => {
                let query = overrides.sql.is_some();
                spec.approved =
                    !source.discovers_tables() || query || source.include.contains(&qualified);
                spec.tunnel = project
                    .connections
                    .get(&source.connection)
                    .filter(|_| {
                        matches!(source.kind, SourceKind::Postgres | SourceKind::PostgresCdc)
                    })
                    .and_then(|connection| match connection {
                        ConnectionConfig::Postgres { ssh, .. } => {
                            ssh.as_ref().map(SshTunnel::route)
                        }
                        _ => None,
                    });
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

/// Where a source's tables go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedTarget {
    pub(crate) name: String,
    pub(crate) warehouse: Warehouse,
    pub(crate) database: Option<String>,
    pub(crate) schema: String,
    /// The table folder template, for a files target.
    pub(crate) path: Option<String>,
    /// How its tables name the columns a load adds, unless a source or table says.
    pub(crate) naming: Option<Naming>,
}

/// The target `source` loads into: its own, the project default, or the only target.
pub(crate) fn source_target(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<ResolvedTarget, String> {
    let name = source
        .destination
        .clone()
        .or_else(|| project.default_target.clone())
        .or_else(|| {
            (project.targets.len() == 1)
                .then(|| project.targets.keys().next().cloned())
                .flatten()
        })
        .ok_or("the source has no destination")?;
    let target = project
        .targets
        .get(&name)
        .ok_or_else(|| format!("target {name:?} does not exist"))?;
    let connection = project
        .connections
        .get(&target.connection)
        .ok_or_else(|| format!("connection {:?} does not exist", target.connection))?;
    let Some((warehouse, database)) = connection_warehouse(connection) else {
        return Err(format!(
            "target {name:?} uses a {} connection, which is not a destination",
            connection.kind()
        ));
    };
    if !sql_adapter(warehouse).runs_sql() {
        return Ok(ResolvedTarget {
            name,
            warehouse,
            database: None,
            schema: String::new(),
            path: Some(target.path_template().to_owned()),
            naming: target.naming,
        });
    }
    let schema = source
        .schema
        .clone()
        .or_else(|| target.loader_schema.clone())
        .ok_or_else(|| format!("target {name:?} has no loader_schema"))?;
    Ok(ResolvedTarget {
        name,
        warehouse,
        database: match connection {
            ConnectionConfig::MotherDuck { database, .. } => target
                .database
                .as_deref()
                .or(database.as_deref())
                .and_then(|database| motherduck_path(database).name)
                .map(str::to_owned),
            _ => target.database.clone().or(database),
        },
        schema,
        path: None,
        naming: target.naming,
    })
}
