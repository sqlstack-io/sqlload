//! An ODBC source's reader, from its connection's settings.

use crate::auth::models::Secret;
use crate::odbc_source::models::{OdbcReader, ValueLimits};

/// A reader connecting with `connection_string` as given, or with `DSN=`, `UID=` and `PWD=` built from `dsn`, `user` and `password` (each value in braces, so `;` and `=` in it stay literal); nothing connects until the first read.
pub fn odbc_reader(
    (connection_string, dsn): (Option<&Secret>, Option<&str>),
    (user, password): (Option<&str>, Option<&Secret>),
    limits: ValueLimits,
) -> Result<OdbcReader, String> {
    let braced = |value: &str| format!("{{{}}}", value.replace('}', "}}"));
    let connection_string = match (connection_string, dsn) {
        (Some(text), None) => text.clone(),
        (None, Some(dsn)) => {
            let mut parts = vec![format!("DSN={}", braced(dsn))];
            parts.extend(user.map(|user| format!("UID={}", braced(user))));
            parts.extend(password.map(|password| format!("PWD={}", braced(password.expose()))));
            Secret(parts.join(";"))
        }
        _ => {
            return Err(
                "an odbc connection takes connection_string, or dsn (with user and password if the DSN has none), not both".to_owned(),
            );
        }
    };
    Ok(OdbcReader {
        connection_string,
        limits,
    })
}
