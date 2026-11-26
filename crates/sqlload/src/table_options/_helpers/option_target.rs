use crate::adapters::dispatch::main::dialect::connection_warehouse::connection_warehouse;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::project::models::{ProjectConfig, SourceConfig};
use crate::table_options::models::OptionTarget;

/// What `source`'s options read of the target it loads into (its own, the project default, or the only target); `None` when that target or its connection is missing or no destination, which parse reports itself.
pub(crate) fn option_target(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Option<OptionTarget> {
    let only = (project.targets.len() == 1)
        .then(|| project.targets.keys().next().cloned())
        .flatten();
    let name = source
        .destination
        .clone()
        .or_else(|| project.default_target.clone())
        .or(only)?;
    let target = project.targets.get(&name)?;
    let (warehouse, _) = connection_warehouse(project.connections.get(&target.connection)?)?;
    Some(OptionTarget {
        warehouse,
        path: (!sql_adapter(warehouse).runs_sql()).then(|| target.path_template().to_owned()),
        naming: target.naming,
        compat: target.compat,
    })
}
