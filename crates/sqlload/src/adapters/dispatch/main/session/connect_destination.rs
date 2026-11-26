//! Open the destination one target uses.

use crate::adapters::dispatch::main::session::_own_destination::own_destination;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::session::main::_connect_session::connect_session;
use crate::loader::errors::LoaderError;
use crate::project::models::{ConnectionConfig, ProjectConfig};

/// The open destination of target `name`: its caller's session, or a connection of SQLLoad's own.
pub async fn connect_destination(
    project: &ProjectConfig,
    name: &str,
) -> Result<Destination, LoaderError> {
    let target = project
        .targets
        .get(name)
        .ok_or_else(|| LoaderError::Invalid(format!("target {name:?} does not exist")))?;
    match project.connections.get(&target.connection) {
        Some(ConnectionConfig::Session(session)) => Ok(Destination::Session(Box::new(
            connect_session(session, target.database.as_deref()).await?,
        ))),
        connection => own_destination(project, (name, target), connection),
    }
}
