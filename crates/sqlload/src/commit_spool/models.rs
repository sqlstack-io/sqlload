//! A table's held batches.

use std::path::PathBuf;

/// The batches of a table's commit held until it is sent, in a folder of their own that goes with the spool.
#[derive(Debug)]
pub struct Spool {
    pub folder: tempfile::TempDir,
    pub held: Vec<HeldBatch>,
    /// Files made so far, which names the next.
    pub made: u64,
}

/// One batch of the table's load, as the publisher numbers them, held in files: one per part of its rows, in the order they came.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldBatch {
    pub batch: u64,
    pub files: Vec<PathBuf>,
    /// The bytes its files take.
    pub bytes: usize,
}
