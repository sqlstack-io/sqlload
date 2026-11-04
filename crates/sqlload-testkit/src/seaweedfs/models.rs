use testcontainers::{ContainerAsync, GenericImage};

/// A running SeaweedFS server; the container stops when this is dropped.
#[derive(Debug)]
pub struct SeaweedFs {
    /// The S3 endpoint, `http://127.0.0.1:<port>`.
    pub endpoint: String,
    pub container: ContainerAsync<GenericImage>,
}
