//! The reader for an `odbc` source.

use crate::odbc_source::main::_odbc_reader::odbc_reader;
use crate::odbc_source::models::{OdbcReader, ValueLimits};
use crate::project::models::{ConnectionConfig, ProjectConfig, SourceConfig};

/// A reader for `source`'s `odbc` connection, with the source's value caps; nothing connects until the first read.
pub fn source_reader(project: &ProjectConfig, source: &SourceConfig) -> Result<OdbcReader, String> {
    let Some(ConnectionConfig::Odbc {
        connection_string,
        dsn,
        user,
        password,
    }) = project.connections.get(&source.connection)
    else {
        return Err(format!(
            "connection {:?} is not an odbc connection",
            source.connection
        ));
    };
    let limits = ValueLimits {
        max_text_size: source.max_text_size,
        max_binary_size: source.max_binary_size,
        long_column_threshold: source
            .long_column_threshold
            .map(|size| usize::try_from(size.0).unwrap_or(usize::MAX)),
    };
    odbc_reader(
        (connection_string.as_ref(), dsn.as_deref()),
        (user.as_deref(), password.as_ref()),
        limits,
    )
}
