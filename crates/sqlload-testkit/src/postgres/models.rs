use testcontainers::{ContainerAsync, GenericImage};

/// A running Postgres server; the container stops when this is dropped.
#[derive(Debug)]
pub struct Postgres {
    pub host: String,
    pub port: u16,
    pub container: ContainerAsync<GenericImage>,
}

/// A TLS-only Postgres server and the folder holding `ca.pem` (its CA), `other_ca.pem` (an unrelated CA), and `client.pem` and `client.key` (a client certificate for the certificate user).
#[derive(Debug)]
pub struct TlsPostgres {
    pub postgres: Postgres,
    pub files: tempfile::TempDir,
}
