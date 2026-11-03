use testcontainers::{ContainerAsync, GenericImage};

/// A running Redpanda broker; the container stops when this is dropped.
#[derive(Debug)]
pub struct Redpanda {
    pub brokers: String,
    pub container: ContainerAsync<GenericImage>,
}
