//! `COPY` a files source's files straight from the external stage.

use crate::adapters::snowflake::_helpers::dialect::copy_select::copy_sql;
use crate::adapters::snowflake::constants::{MAX_COPY_SQL_BYTES, MAX_FILES_PER_COPY};
use crate::adapters::snowflake::models::{CopyBatch, DirectCopy};
use crate::files_source::models::DataFile;

/// `COPY` statements loading `request.files` into `request.target`, at most 1,000 files and about 900 KiB of SQL each: data columns read by name and cast (JSON parsed), partition values, source file, version per file (matched on `METADATA$FILENAME`, which on an external stage is the bucket-relative key and on the user stage `@~/` and the folder, failing on any other file) and row number, and literals; always `FORCE = TRUE` and `ON_ERROR = ABORT_STATEMENT`.
pub(in crate::adapters) fn direct_copies(
    request: &DirectCopy<'_>,
) -> Result<Vec<CopyBatch>, String> {
    let mut groups: Vec<(Option<&[String]>, Vec<DataFile>)> = Vec::new();
    for data in request.files {
        let header = request
            .headers
            .get(&data.file.relative_key)
            .map(Vec::as_slice);
        match groups.iter_mut().find(|(seen, _)| *seen == header) {
            Some((_, files)) => files.push(data.clone()),
            None => groups.push((header, vec![data.clone()])),
        }
    }
    let mut batches: Vec<CopyBatch> = Vec::new();
    let chunks: Vec<(Option<&[String]>, &[DataFile])> = groups
        .iter()
        .flat_map(|(header, files)| {
            std::iter::repeat(*header).zip(files.chunks(MAX_FILES_PER_COPY))
        })
        .collect();
    for (header, chunk) in chunks {
        let mut pending: Vec<&[DataFile]> = vec![chunk];
        while let Some(files) = pending.pop() {
            let sql = copy_sql(request, header, files)?;
            match (sql.len() > MAX_COPY_SQL_BYTES, files.len()) {
                (true, count) if count > 1 => {
                    let (first, second) = files.split_at(count / 2);
                    pending.push(second);
                    pending.push(first);
                }
                _ => batches.push(CopyBatch {
                    sql,
                    keys: files
                        .iter()
                        .map(|data| data.file.relative_key.clone())
                        .collect(),
                }),
            }
        }
    }
    Ok(batches)
}
