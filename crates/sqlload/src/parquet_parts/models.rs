use arrow_schema::SchemaRef;
use parquet::arrow::ArrowWriter;

/// The Parquet file being written, if any, and the bytes of the files finished so far.
#[derive(Debug, Default)]
pub struct PartWriter {
    pub open: Option<ArrowWriter<Vec<u8>>>,
    /// The open file's schema; a batch with another schema starts a new file.
    pub schema: Option<SchemaRef>,
    /// Encoded bytes of every file finished by this writer.
    pub finished_bytes: usize,
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
