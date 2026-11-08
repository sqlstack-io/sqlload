//! Open a Postgres target.

use std::sync::Arc;

use crate::adapters::postgres::models::PostgresDestination;
use crate::loader::errors::LoaderError;
use crate::project::models::{ConnectionConfig, ProjectConfig};

/// The Postgres target on `connection`, connecting to `database` instead of the connection's when set; nothing connects until the first statement.
pub(in crate::adapters) fn open_postgres(
    project: &ProjectConfig,
    connection: &str,
    database: Option<&str>,
) -> Result<PostgresDestination, LoaderError> {
    let Some(ConnectionConfig::Postgres { .. }) = project.connections.get(connection) else {
        return Err(LoaderError::Invalid(format!(
            "connection {connection:?} is not a Postgres connection"
        )));
    };
    Ok(PostgresDestination {
        project: Arc::new(project.clone()),
        connection: connection.to_owned(),
        database: database.map(str::to_owned),
        session: Arc::default(),
        reconnects: true,
        ddl: Arc::default(),
    })
}
