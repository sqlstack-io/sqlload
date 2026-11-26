//! Open a destination on the caller's connection.

use std::sync::Arc;

use crate::adapters::session::_helpers::blocking::opened;
use crate::adapters::session::models::{SessionConnection, SessionDestination};
use crate::column_types::models::Warehouse;
use crate::loader::errors::LoaderError;

/// The destination a target on `connection` loads into, its names qualified with `database` (the target's), else the connection's; its session opens now, so a connection that cannot be reached fails before planning.
pub(in crate::adapters) async fn connect_session(
    connection: &SessionConnection,
    database: Option<&str>,
) -> Result<SessionDestination, LoaderError> {
    if connection.warehouse == Warehouse::Parquet {
        return Err(LoaderError::Invalid(
            "a caller's session loads into a warehouse; files targets write through SQLLoad's own connections".to_owned(),
        ));
    }
    let destination = SessionDestination {
        connection: connection.clone(),
        database: database
            .map(str::to_owned)
            .or_else(|| connection.database.clone()),
        session: Arc::default(),
        ddl: Arc::default(),
    };
    opened(&destination).await?;
    Ok(destination)
}
