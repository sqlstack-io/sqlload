/// Why a store operation failed.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("cannot open {location}: {source}")]
    Open {
        location: String,
        source: object_store::Error,
    },
    #[error("{action} {key}: {source}")]
    Object {
        action: &'static str,
        key: String,
        source: object_store::Error,
    },
}
