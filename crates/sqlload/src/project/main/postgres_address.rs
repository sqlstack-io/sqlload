//! Where and how a Postgres connection connects.

use crate::postgres_source::models::{PostgresAddress, PostgresTls};
use crate::project::models::{ConnectionConfig, ProjectConfig};

/// Postgres connection `name`'s address and TLS settings, or why it has none.
pub fn postgres_address<'a>(
    project: &'a ProjectConfig,
    name: &str,
) -> Result<PostgresAddress<'a>, String> {
    let Some(ConnectionConfig::Postgres {
        host,
        port,
        user,
        password,
        database,
        sslmode,
        sslrootcert,
        sslcert,
        sslkey,
        ssh,
    }) = project.connections.get(name)
    else {
        return Err(format!("connection {name:?} is not a Postgres connection"));
    };
    if ssh.is_some() && host.starts_with('/') {
        return Err(format!(
            "connection {name:?} sets ssh, so its host must be a host name or address the bastion can reach, not the Unix socket folder {host}"
        ));
    }
    Ok(PostgresAddress {
        host,
        port: *port,
        user,
        password: password.as_ref().map(|secret| secret.expose()),
        database,
        tls: PostgresTls {
            mode: *sslmode,
            root_cert: sslrootcert.as_deref(),
            cert: sslcert.as_deref(),
            key: sslkey.as_deref(),
        },
        ssh: ssh.as_ref(),
    })
}
