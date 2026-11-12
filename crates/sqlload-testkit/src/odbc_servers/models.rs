use testcontainers::{ContainerAsync, GenericImage};

/// A running database server and the ODBC connection string that reaches it; the container stops when this is dropped.
#[derive(Debug)]
pub struct OdbcServer {
    pub connection_string: String,
    pub container: ContainerAsync<GenericImage>,
}
