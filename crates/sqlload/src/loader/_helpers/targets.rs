use crate::column_types::models::Warehouse;
use crate::project::models::{ConnectionConfig, ProjectConfig, SourceConfig};

/// Where a source's tables go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedTarget {
    pub(crate) name: String,
    pub(crate) warehouse: Warehouse,
    pub(crate) database: Option<String>,
    pub(crate) schema: String,
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
    let (warehouse, database) = match connection {
        ConnectionConfig::Snowflake { database, .. } => {
            (Warehouse::Snowflake, Some(database.clone()))
        }
        ConnectionConfig::DuckDb { .. } => (Warehouse::DuckDb, None),
        ConnectionConfig::Postgres { .. } => {
            return Err(format!(
                "target {name:?} uses a Postgres connection, which is not a destination yet"
            ));
        }
    };
    Ok(ResolvedTarget {
        name,
        warehouse,
        database: target.database.clone().or(database),
        schema: source
            .schema
            .clone()
            .unwrap_or_else(|| target.loader_schema.clone()),
    })
}
