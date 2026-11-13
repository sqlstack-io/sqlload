//! Read the columns of a files source's newest files, to work out its schema.

use std::collections::BTreeSet;

use futures_util::{StreamExt, TryStreamExt};

use crate::file_store::models::OpenFolder;
use crate::files_source::_helpers::parquet::footers::parquet_footer;
use crate::files_source::_helpers::plan::columns::{SampledColumn, parquet_columns};
use crate::files_source::_helpers::text::sample::sample_file;
use crate::files_source::constants::{
    CONCURRENT_FOOTER_READS, CONCURRENT_TEXT_SAMPLES, SCHEMA_SAMPLE_FILES,
};
use crate::files_source::errors::FilesError;
use crate::files_source::models::{Codec, DataFile, FilesEncoding, SourceFormat};

/// The columns of the newest `SCHEMA_SAMPLE_FILES` of `files`, newest first, read a few at a time (more for small Parquet footers than for JSON and CSV samples); and the objects flattening replaced in any of them, sorted.
pub(crate) async fn newest_columns(
    folder: &OpenFolder,
    files: &[(DataFile, Codec)],
    encoding: &FilesEncoding,
) -> Result<(Vec<Vec<SampledColumn>>, Vec<String>), FilesError> {
    let at_once = match encoding.format {
        SourceFormat::Parquet => CONCURRENT_FOOTER_READS,
        _ => CONCURRENT_TEXT_SAMPLES,
    };
    let sampled: Vec<(Vec<SampledColumn>, Vec<String>)> =
        futures_util::stream::iter(files.iter().rev().take(SCHEMA_SAMPLE_FILES))
            .map(|(data, codec)| file_columns_of(folder, data, *codec, encoding))
            .buffered(at_once)
            .try_collect()
            .await?;
    let (columns, objects): (Vec<Vec<SampledColumn>>, Vec<Vec<String>>) =
        sampled.into_iter().unzip();
    let objects: BTreeSet<String> = objects.into_iter().flatten().collect();
    Ok((columns, objects.into_iter().collect()))
}

/// One file's columns and flattened objects: from its Parquet footer (none), or from its first JSON or CSV records.
async fn file_columns_of(
    folder: &OpenFolder,
    data: &DataFile,
    codec: Codec,
    encoding: &FilesEncoding,
) -> Result<(Vec<SampledColumn>, Vec<String>), FilesError> {
    match encoding.format {
        SourceFormat::Parquet => Ok((
            parquet_columns(&parquet_footer(folder, &data.file).await?),
            Vec::new(),
        )),
        _ => sample_file(folder, &data.file, codec, encoding)
            .await
            .map_err(|reason| FilesError::File {
                key: data.file.relative_key.clone(),
                reason,
            }),
    }
}
