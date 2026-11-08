/// Which store a location is in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreKind {
    S3 {
        bucket: String,
        region: Option<String>,
        /// A custom endpoint (MinIO, LocalStack, tests).
        endpoint: Option<String>,
        /// The shared credentials file's profile to take keys from, over `AWS_PROFILE`.
        profile: Option<String>,
    },
    /// A directory on the machine SQLLoad runs on.
    Filesystem { root: String },
}

/// A folder in a store: the store, and a prefix within it that is empty or ends in `/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    pub store: StoreKind,
    pub prefix: String,
}

impl std::fmt::Display for Location {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.store {
            StoreKind::S3 { bucket, .. } => write!(formatter, "s3://{bucket}/{}", self.prefix),
            StoreKind::Filesystem { root } => {
                write!(formatter, "{}/{}", root.trim_end_matches('/'), self.prefix)
            }
        }
    }
}
