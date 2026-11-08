use testcontainers::{ContainerAsync, GenericImage};

/// A running ClickHouse server; the container stops when this is dropped.
#[derive(Debug)]
pub struct ClickHouse {
    /// The host port of its HTTP interface.
    pub port: u16,
    pub container: ContainerAsync<GenericImage>,
}
