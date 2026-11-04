use tokio_postgres::Client;

use crate::loader::errors::LoaderError;
use crate::postgres_source::main::connect_postgres::connect_postgres;
use crate::postgres_source::models::PostgresAddress;
use crate::project::models::{ConnectionConfig, ProjectConfig, SourceConfig};

/// A client for a source's Postgres connection.
pub(crate) async fn connect_source(
    project: &ProjectConfig,
    source: &SourceConfig,
) -> Result<Client, LoaderError> {
    let Some(ConnectionConfig::Postgres {
        host,
        port,
        user,
        password,
        database,
    }) = project.connections.get(&source.connection)
    else {
        return Err(LoaderError::Invalid(format!(
            "connection {:?} is not a Postgres connection",
            source.connection
        )));
    };
    let address = PostgresAddress {
        host,
        port: *port,
        user,
        password: password.as_ref().map(|secret| secret.expose()),
        database,
    };
    Ok(connect_postgres(address).await?)
}

/// Whether `selectors` pick a table, by source name, destination name or `schema.table`.
pub(crate) fn selected(
    selectors: &[String],
    source: &str,
    destination: &str,
    source_table: &str,
) -> bool {
    selectors.is_empty()
        || selectors.iter().any(|selector| {
            selector == source || selector == destination || selector == source_table
        })
}
