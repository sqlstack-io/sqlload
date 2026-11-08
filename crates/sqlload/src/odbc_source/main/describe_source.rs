//! What `debug` says about an ODBC source.

use crate::odbc_source::_helpers::connect::connect;
use crate::odbc_source::errors::OdbcSourceError;
use crate::odbc_source::models::OdbcReader;

/// The database the source reads, as its driver names it, and its current catalog.
pub async fn describe_source(reader: &OdbcReader) -> Result<String, OdbcSourceError> {
    let reader = reader.clone();
    tokio::task::spawn_blocking(move || {
        let connection = connect(&reader)?;
        let dbms = connection.database_management_system_name()?;
        let catalog = connection.current_catalog().unwrap_or_default();
        Ok(match catalog.is_empty() {
            true => format!("{dbms} through ODBC"),
            false => format!("{dbms} through ODBC, catalog {catalog}"),
        })
    })
    .await
    .map_err(|error| OdbcSourceError::Invalid(format!("the ODBC check stopped: {error}")))?
}
