use arrow_schema::SchemaRef;
use parquet::arrow::ArrowWriter;

/// The Parquet file being written, if any, and the bytes of the files finished so far.
#[derive(Default)]
pub struct PartWriter {
    pub open: Option<ArrowWriter<Vec<u8>>>,
    /// The open file's schema; a batch with another schema starts a new file.
    pub schema: Option<SchemaRef>,
    /// Encoded bytes of every file finished by this writer.
    pub finished_bytes: usize,
}

impl std::fmt::Debug for PartWriter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PartWriter")
            .field("open", &self.open.is_some())
            .field("finished_bytes", &self.finished_bytes)
            .finish()
    }
}

impl PartWriter {
    /// About how many bytes the files hold: finished, plus the open file's estimate.
    pub fn bytes(&self) -> usize {
        self.finished_bytes
            + self.open.as_ref().map_or(0, |writer| {
                writer.bytes_written() + writer.in_progress_size()
            })
    }
}
