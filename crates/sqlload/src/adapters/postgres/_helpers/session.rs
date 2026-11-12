//! A Postgres target's sessions.

use tokio_postgres::Client;

use crate::adapters::postgres::models::PostgresDestination;
use crate::loader::errors::LoaderError;
use crate::postgres_source::errors::PostgresError;
use crate::postgres_source::main::connect_postgres::connect_postgres;
use crate::postgres_source::models::Encryption;
use crate::project::main::postgres_address::postgres_address;

/// A new session on the target, with the address and TLS settings a Postgres source on the connection uses (the target's database instead of the connection's when it names one), and what protects it.
async fn connect_session(
    destination: &PostgresDestination,
) -> Result<(Client, Encryption), LoaderError> {
    let mut address = postgres_address(&destination.project, &destination.connection)
        .map_err(LoaderError::Invalid)?;
    if let Some(database) = &destination.database {
        address.database = database;
    }
    Ok(connect_postgres(address).await?)
}

/// The target's session and what protects it, opened on first use. The shared session opens again when it closed; a table's own session does not, since its temporary staging table and open transaction went with it, so its load fails instead.
pub(crate) async fn session(
    destination: &PostgresDestination,
) -> Result<tokio::sync::OwnedMutexGuard<Option<(Client, Encryption)>>, LoaderError> {
    let mut held = destination.session.clone().lock_owned().await;
    let closed = held.as_ref().map(|(client, _)| client.is_closed());
    match (closed, destination.reconnects) {
        (None, _) | (Some(true), true) => *held = Some(connect_session(destination).await?),
        (Some(true), false) => {
            return Err(LoaderError::Transient(
                "the table's Postgres session closed, and its staging table with it; nothing it had not committed was written".to_owned(),
            ));
        }
        (Some(false), _) => {}
    }
    Ok(held)
}

/// A Postgres error as the loader reports it.
pub(crate) fn query_error(error: tokio_postgres::Error) -> LoaderError {
    LoaderError::Postgres(PostgresError::Query(error))
}
