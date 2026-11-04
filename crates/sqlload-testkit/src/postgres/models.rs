use testcontainers::{ContainerAsync, GenericImage};

/// A running Postgres server; the container stops when this is dropped.
#[derive(Debug)]
pub struct Postgres {
    pub host: String,
    pub port: u16,
    pub container: ContainerAsync<GenericImage>,
}
