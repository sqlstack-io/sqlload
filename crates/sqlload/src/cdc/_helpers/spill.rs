//! Changes kept in an anonymous local file once a batch outgrows memory: written in stream order, read back in order.

use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};

use crate::cdc::models::{Change, ChangeOp, TupleValue};

/// A local disk error, as the load reports it.
fn io(error: std::io::Error) -> String {
    format!("cannot spill changes to local disk: {error}")
}

/// An append-only file of records: raw pgoutput messages, or changes.
#[derive(Debug)]
pub(crate) struct SpillFile {
    writer: BufWriter<std::fs::File>,
    /// Records written.
    pub(crate) count: usize,
    /// Bytes written.
    pub(crate) bytes: usize,
    /// Whether it has been read back, after which nothing more may be written (the reader shares its offset).
    read: bool,
}

impl SpillFile {
    /// A new file in the system's temporary folder, removed when dropped.
    pub(crate) fn new() -> Result<Self, String> {
        Ok(Self {
            writer: BufWriter::new(tempfile::tempfile().map_err(io)?),
            count: 0,
            bytes: 0,
            read: false,
        })
    }

    /// Append one raw message.
    pub(crate) fn push_bytes(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.write(&length_prefixed(bytes)?)
    }

    /// Append one change.
    pub(crate) fn push_change(&mut self, change: &Change) -> Result<(), String> {
        let op = match change.op {
            ChangeOp::Insert => 0,
            ChangeOp::Update => 1,
            ChangeOp::Delete => 2,
            ChangeOp::Truncate => 3,
        };
        let mut record: Vec<u8> = [op]
            .into_iter()
            .chain(change.lsn.to_le_bytes())
            .chain(change.seq.to_le_bytes())
            .chain(change.committed_at.to_le_bytes())
            .collect();
        record.extend(values_bytes(&change.values)?);
        match &change.old_key {
            Some(old) => {
                record.push(1);
                record.extend(values_bytes(old)?);
            }
            None => record.push(0),
        }
        self.write(&record)
    }

    /// Append one encoded record.
    fn write(&mut self, record: &[u8]) -> Result<(), String> {
        if self.read {
            return Err("a spill file was written after it was read".to_owned());
        }
        self.writer.write_all(record).map_err(io)?;
        self.count += 1;
        self.bytes += record.len();
        Ok(())
    }

    /// Read the records back from the start; nothing is written after.
    pub(crate) fn read(&mut self) -> Result<SpillReader, String> {
        self.read = true;
        self.writer.flush().map_err(io)?;
        let mut file = self.writer.get_ref().try_clone().map_err(io)?;
        file.seek(SeekFrom::Start(0)).map_err(io)?;
        Ok(SpillReader {
            reader: BufReader::new(file),
            left: self.count,
        })
    }
}

/// A spilled file's records, in the order written.
#[derive(Debug)]
pub(crate) struct SpillReader {
    reader: BufReader<std::fs::File>,
    left: usize,
}

impl SpillReader {
    /// The next raw message, or `None` at the end.
    pub(crate) fn next_bytes(&mut self) -> Result<Option<Vec<u8>>, String> {
        if self.left == 0 {
            return Ok(None);
        }
        self.left -= 1;
        self.read_bytes().map(Some)
    }

    /// The next change, or `None` at the end.
    pub(crate) fn next_change(&mut self) -> Result<Option<Change>, String> {
        if self.left == 0 {
            return Ok(None);
        }
        self.left -= 1;
        let mut header = [0u8; 25];
        self.reader.read_exact(&mut header).map_err(io)?;
        let number = |at: usize| {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&header[at..at + 8]);
            i64::from_le_bytes(bytes)
        };
        let op = match header[0] {
            0 => ChangeOp::Insert,
            1 => ChangeOp::Update,
            2 => ChangeOp::Delete,
            3 => ChangeOp::Truncate,
            tag => return Err(format!("a spill file holds an unknown change tag {tag}")),
        };
        let values = self.read_values()?;
        let mut flag = [0u8; 1];
        self.reader.read_exact(&mut flag).map_err(io)?;
        let old_key = match flag[0] {
            1 => Some(self.read_values()?),
            _ => None,
        };
        Ok(Some(Change {
            op,
            values,
            old_key,
            lsn: number(1),
            seq: number(9),
            committed_at: number(17),
        }))
    }

    /// One length-prefixed run of bytes.
    fn read_bytes(&mut self) -> Result<Vec<u8>, String> {
        let mut length = [0u8; 4];
        self.reader.read_exact(&mut length).map_err(io)?;
        let mut bytes = vec![0u8; u32::from_le_bytes(length) as usize];
        self.reader.read_exact(&mut bytes).map_err(io)?;
        Ok(bytes)
    }

    /// One list of values, as [`values_bytes`] wrote it.
    fn read_values(&mut self) -> Result<Vec<TupleValue>, String> {
        let mut count = [0u8; 4];
        self.reader.read_exact(&mut count).map_err(io)?;
        let count = u32::from_le_bytes(count) as usize;
        let mut values: Vec<TupleValue> = Vec::with_capacity(count);
        for _ in 0..count {
            let mut tag = [0u8; 1];
            self.reader.read_exact(&mut tag).map_err(io)?;
            values.push(match tag[0] {
                0 => TupleValue::Null,
                1 => TupleValue::Unchanged,
                2 => TupleValue::Text(
                    String::from_utf8(self.read_bytes()?).map_err(|error| error.to_string())?,
                ),
                tag => return Err(format!("a spill file holds an unknown value tag {tag}")),
            });
        }
        Ok(values)
    }
}

/// `bytes` after its length.
fn length_prefixed(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let length = u32::try_from(bytes.len()).map_err(|error| error.to_string())?;
    Ok(length
        .to_le_bytes()
        .into_iter()
        .chain(bytes.iter().copied())
        .collect())
}

/// `values` after their count, each a tag and, for text, its length-prefixed bytes.
fn values_bytes(values: &[TupleValue]) -> Result<Vec<u8>, String> {
    let count = u32::try_from(values.len()).map_err(|error| error.to_string())?;
    let mut bytes: Vec<u8> = count.to_le_bytes().to_vec();
    for value in values {
        match value {
            TupleValue::Null => bytes.push(0),
            TupleValue::Unchanged => bytes.push(1),
            TupleValue::Text(text) => {
                bytes.push(2);
                bytes.extend(length_prefixed(text.as_bytes())?);
            }
        }
    }
    Ok(bytes)
}
