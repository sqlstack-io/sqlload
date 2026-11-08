//! Connections through the process's one ODBC environment.

use arrow_odbc::odbc_api::{Connection, ConnectionOptions, environment};

use crate::odbc_source::constants::{IDENTIFIER_QUOTE, NO_QUOTE};
use crate::odbc_source::errors::OdbcSourceError;
use crate::odbc_source::models::OdbcReader;

/// A connection with the reader's connection string, refused when its driver quotes identifiers other than as ANSI SQL does, since SQLLoad writes `"name"`.
pub(crate) fn connect(reader: &OdbcReader) -> Result<Connection<'static>, OdbcSourceError> {
    let connection = environment()?.connect_with_connection_string(
        reader.connection_string.expose(),
        ConnectionOptions::default(),
    )?;
    match connection.identifier_quote_char()? {
        None | Some(IDENTIFIER_QUOTE | NO_QUOTE) => Ok(connection),
        Some(quote) => Err(OdbcSourceError::Invalid(format!(
            "the driver quotes identifiers with {quote:?}; SQLLoad's ODBC source writes ANSI double quotes, so switch the driver to them (for example ANSI_QUOTES)"
        ))),
    }
}
