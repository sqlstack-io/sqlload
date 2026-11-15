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

    /// Hold `rows`, part of batch `batch`, in a local file of their own, zstd-compressed so its size is close to the Parquet a staging writer would make of it.
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
            .try_with_compression(Some(CompressionType::ZSTD))
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
        let bytes = std::fs::metadata(&path)
            .map(|metadata| usize::try_from(metadata.len()).unwrap_or(usize::MAX))
            .map_err(|error| failed(error.to_string()))?;
        match self.held.last_mut().filter(|held| held.batch == batch) {
            Some(held) => {
                held.files.push(path);
                held.bytes += bytes;
            }
            None => self.held.push(HeldBatch {
                batch,
                files: vec![path],
                bytes,
            }),
        }
        Ok(())
    }

    /// Every batch held so far, to send; the spool holds none after.
    pub(crate) fn take(&mut self) -> Vec<HeldBatch> {
        std::mem::take(&mut self.held)
    }

    /// The batches held before batch `batch`, which have ended, to send; the spool keeps the rest.
    pub(crate) fn take_ended(&mut self, batch: u64) -> Vec<HeldBatch> {
        let (ended, open) = std::mem::take(&mut self.held)
            .into_iter()
            .partition(|held| held.batch < batch);
        self.held = open;
        ended
    }

    /// The files held so far of batch `batch`, still open, to write without ending it; the spool keeps the batch, empty, so the rows it holds next join it and its end still loads it.
    pub(crate) fn take_open_files(&mut self, batch: u64) -> Vec<std::path::PathBuf> {
        match self.held.last_mut().filter(|held| held.batch == batch) {
            Some(held) => {
                held.bytes = 0;
                std::mem::take(&mut held.files)
            }
            None => Vec::new(),
        }
    }

    /// The bytes held of batch `batch`.
    pub(crate) fn held_bytes(&self, batch: u64) -> usize {
        self.held
            .iter()
            .filter(|held| held.batch == batch)
            .map(|held| held.bytes)
            .sum()
    }
}
