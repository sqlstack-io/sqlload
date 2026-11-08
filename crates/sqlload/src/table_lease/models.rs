/// A load's claim on a files table: only the newest generation's holder may write the table while it is unexpired.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub generation: u64,
    pub load_id: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
}
