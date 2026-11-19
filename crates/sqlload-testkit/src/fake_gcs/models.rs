use testcontainers::{ContainerAsync, GenericImage};

/// A running fake GCS server; the container stops when this is dropped.
#[derive(Debug)]
pub struct FakeGcs {
    /// Its base URL, `http://127.0.0.1:<port>`.
    pub endpoint: String,
    pub container: ContainerAsync<GenericImage>,
}
