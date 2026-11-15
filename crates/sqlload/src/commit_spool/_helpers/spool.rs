//! Holding batches, and taking them to send.

use arrow_array::RecordBatch;
use arrow_ipc::CompressionType;
use arrow_ipc::writer::{FileWriter, IpcWriteOptions};

use crate::commit_spool::constants::{FILE_SUFFIX, FOLDER_PREFIX};
use crate::commit_spool::models::{HeldBatch, Spool};

impl Spool {
    /// A spool with its folder made in `parent`.
    pub(crate) fn new(parent: &std::path::Path) -> Result<Self, String> {
        let folder = tempfile::Builder::new()
            .prefix(FOLDER_PREFIX)
            .tempdir_in(parent)
            .map_err(|error| {
                format!(
                    "send = \"at_commit\" cannot make a folder in {} to hold rows until they commit: {error}",
                    parent.display()
                )
            })?;
        Ok(Self {
            folder,
            held: Vec::new(),
            made: 0,
        })
    }

    /// Hold `rows`, part of batch `batch`, in a local file of their own, LZ4-compressed.
    pub(crate) fn hold(&mut self, batch: u64, rows: &RecordBatch) -> Result<(), String> {
        let path = self
            .folder
            .path()
            .join(format!("{}{FILE_SUFFIX}", self.made));
        self.made += 1;
        let failed = |error: String| {
            format!(
                "send = \"at_commit\" cannot hold rows in {}: {error}",
                path.display()
            )
        };
        let file = std::fs::File::create(&path).map_err(|error| failed(error.to_string()))?;
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))
            .map_err(|error| failed(error.to_string()))?;
        let mut writer = FileWriter::try_new_with_options(
            std::io::BufWriter::new(file),
            &rows.schema(),
            options,
        )
        .map_err(|error| failed(error.to_string()))?;
        writer
            .write(rows)
            .and_then(|()| writer.finish())
            .map_err(|error| failed(error.to_string()))?;
        match self.held.last_mut().filter(|held| held.batch == batch) {
            Some(held) => held.files.push(path),
            None => self.held.push(HeldBatch {
                batch,
                files: vec![path],
            }),
        }
        Ok(())
    }

    /// Every batch held so far, to send; the spool holds none after.
    pub(crate) fn take(&mut self) -> Vec<HeldBatch> {
        std::mem::take(&mut self.held)
    }
}
