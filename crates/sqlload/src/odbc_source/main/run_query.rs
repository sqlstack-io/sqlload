//! Run a read's query without fetching its rows.

use crate::odbc_source::_helpers::connect::connect;
use crate::odbc_source::_helpers::parameters::bound_parameters;
use crate::odbc_source::_helpers::query::select_query;
use crate::odbc_source::errors::OdbcSourceError;
use crate::odbc_source::models::{OdbcReadRequest, OdbcReader};

/// Prepare and execute `request`'s `SELECT`, its parameters bound as `read_rows` binds them, and fetch nothing: whether the driver and database take the query and its bounds.
pub async fn run_query(
    reader: &OdbcReader,
    request: &OdbcReadRequest<'_>,
) -> Result<(), OdbcSourceError> {
    let (reader, query, parameters) = (
        reader.clone(),
        select_query(request),
        request.parameters.to_vec(),
    );
    tokio::task::spawn_blocking(move || {
        let connection = connect(&reader)?;
        let dbms: String = connection.database_management_system_name()?;
        let mut prepared = connection.prepare(&query)?;
        let parameters = bound_parameters(&mut prepared, &dbms, &parameters);
        prepared.execute(parameters.as_slice())?;
        Ok(())
    })
    .await
    .map_err(|error| OdbcSourceError::Invalid(format!("the ODBC query stopped: {error}")))?
}
