//! An ODBC source's reader, from its connection's settings.

use crate::auth::models::Secret;
use crate::odbc_source::models::{OdbcReader, ValueLimits};

/// A reader connecting with `connection_string` as given, or with `DSN=` and `UID=` from `dsn` and `user` as written (driver managers read them unbraced; validation refuses `;`, `{` and `}` in them) and `PWD=` from `password` in braces, so any character in it stays literal; nothing connects until the first read.
pub fn odbc_reader(
    (connection_string, dsn): (Option<&Secret>, Option<&str>),
    (user, password): (Option<&str>, Option<&Secret>),
    limits: ValueLimits,
) -> Result<OdbcReader, String> {
    let connection_string = match (connection_string, dsn) {
        (Some(text), None) => text.clone(),
        (None, Some(dsn)) => {
            let mut parts = vec![format!("DSN={dsn}")];
            parts.extend(user.map(|user| format!("UID={user}")));
            parts.extend(
                password
                    .map(|password| format!("PWD={{{}}}", password.expose().replace('}', "}}"))),
            );
            Secret(parts.join(";"))
        }
        _ => {
            return Err(
                "an odbc connection takes connection_string, or dsn, exactly one".to_owned(),
            );
        }
    };
    Ok(OdbcReader {
        connection_string,
        limits,
    })
}
