//! Connections through the process's one ODBC environment.

use arrow_odbc::odbc_api::{Connection, ConnectionOptions, Cursor, environment};

use crate::odbc_source::constants::{ANSI_QUOTES_PROBE, IDENTIFIER_QUOTE, NO_QUOTE};
use crate::odbc_source::errors::OdbcSourceError;
use crate::odbc_source::models::OdbcReader;

/// A connection with the reader's connection string, refused when its database doesn't read `"name"` as a name, since SQLLoad writes ANSI double quotes. A driver that reports another quote is asked once whether the database reads them anyway: MySQL Connector/ODBC reports a backtick even when the session's SQL mode includes `ANSI_QUOTES`.
pub(crate) fn connect(reader: &OdbcReader) -> Result<Connection<'static>, OdbcSourceError> {
    let connection = environment()?.connect_with_connection_string(
        reader.connection_string.expose(),
        ConnectionOptions::default(),
    )?;
    match connection.identifier_quote_char()? {
        None | Some(IDENTIFIER_QUOTE | NO_QUOTE) => Ok(connection),
        Some(_) if reads_ansi_quotes(&connection) => Ok(connection),
        Some(quote) => Err(OdbcSourceError::Invalid(format!(
            "the driver quotes identifiers with {quote:?}; SQLLoad's ODBC source writes ANSI double quotes, so switch the database to them (for MySQL, add INITSTMT=SET SESSION sql_mode=CONCAT(@@SESSION.sql_mode,',ANSI_QUOTES') to the connection string)"
        ))),
    }
}

/// Whether the database reads `"name"` as a column name: the probe returns its row only then (a string literal never equals 1). A failed probe counts as no.
fn reads_ansi_quotes(connection: &Connection<'_>) -> bool {
    match connection.execute(ANSI_QUOTES_PROBE, (), None) {
        Ok(Some(mut cursor)) => matches!(cursor.next_row(), Ok(Some(_))),
        Ok(None) | Err(_) => false,
    }
}
