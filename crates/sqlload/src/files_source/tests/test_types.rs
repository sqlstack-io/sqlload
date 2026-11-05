use crate::column_types::models::ColumnType;
use crate::files_source::models::{Codec, Compression, FilesEncoding, SourceFormat};

pub(crate) struct SelectFilesTestCase {
    pub(crate) description: &'static str,
    /// Loaded files and their versions.
    pub(crate) loaded: &'static [(&'static str, &'static str)],
    pub(crate) cursor_type: Option<ColumnType>,
    pub(crate) bounds: (Option<&'static str>, Option<&'static str>),
    /// The keys chosen, or the error text.
    pub(crate) expected_keys: Result<&'static [&'static str], &'static str>,
}

pub(crate) struct FileSelectionTestCase {
    pub(crate) description: &'static str,
    pub(crate) keys: &'static [&'static str],
    pub(crate) format: SourceFormat,
    pub(crate) compression: Compression,
    /// The keys read as data files, each with the decompression it is read with.
    pub(crate) expected_files: &'static [(&'static str, Codec)],
}

pub(crate) struct FileNamesTestCase {
    pub(crate) description: &'static str,
    pub(crate) format: SourceFormat,
    pub(crate) compression: Compression,
    pub(crate) expected_names: &'static str,
}

pub(crate) struct DiscoverTextFilesTestCase {
    pub(crate) description: &'static str,
    pub(crate) encoding: FilesEncoding,
    /// Files written under the folder, as (key, contents); `.gz` keys are gzip-compressed first.
    pub(crate) files: &'static [(&'static str, &'static str)],
    /// The columns found, as `name:type` in order (`name:!` for one without a type); or the error text.
    pub(crate) expected_columns: Result<&'static str, &'static str>,
    /// The schema problems found, joined by `;`.
    pub(crate) expected_problems: &'static str,
}

pub(crate) struct ReadTextFilesTestCase {
    pub(crate) description: &'static str,
    pub(crate) encoding: FilesEncoding,
    pub(crate) files: &'static [(&'static str, &'static str)],
    /// Files written after the schema is sampled, standing in for files outside the sample; read with the rest.
    pub(crate) unsampled: &'static [(&'static str, &'static str)],
    /// `exclude_columns` patterns.
    pub(crate) exclude: &'static [&'static str],
    /// Types pinned over the inferred ones, by column name.
    pub(crate) pinned: &'static [(&'static str, ColumnType)],
    /// Every row read, as `|`-separated values (`NULL` for NULL) without the source-file columns, one per line; or the error text.
    pub(crate) expected_rows: Result<&'static str, &'static str>,
}

pub(crate) struct MergeColumnsTestCase {
    pub(crate) description: &'static str,
    /// Whether int and float widen to float (JSON and CSV) or conflict (Parquet).
    pub(crate) widen_numbers: bool,
    pub(crate) newer: ColumnType,
    pub(crate) older: ColumnType,
    pub(crate) expected_type: ColumnType,
    pub(crate) expected_problems: usize,
}

pub(crate) struct SplitJsonTestCase {
    pub(crate) description: &'static str,
    pub(crate) by_line: bool,
    pub(crate) max_record_bytes: usize,
    /// The file, fed in chunks of this many bytes.
    pub(crate) text: &'static str,
    pub(crate) chunk_bytes: usize,
    /// The lines the objects start on, or the error text.
    pub(crate) expected_lines: Result<&'static [u64], &'static str>,
}

pub(crate) struct TrackCsvTestCase {
    pub(crate) description: &'static str,
    pub(crate) max_record_bytes: usize,
    pub(crate) text: &'static str,
    pub(crate) chunk_bytes: usize,
    /// The first record's fields joined by `|`, or the error text from reading the first record or following the whole file.
    pub(crate) expected_first: Result<&'static str, &'static str>,
}

pub(crate) struct SkippedFilesTestCase {
    pub(crate) description: &'static str,
    pub(crate) encoding: FilesEncoding,
    pub(crate) files: &'static [(&'static str, &'static str)],
    /// Files read and files skipped.
    pub(crate) expected_counts: (usize, usize),
}

pub(crate) struct LoaderColumnsTestCase {
    pub(crate) description: &'static str,
    /// The columns a file has.
    pub(crate) found: &'static [&'static str],
    /// The columns kept, then those dropped as the file spells them.
    pub(crate) expected_columns: (&'static [&'static str], &'static [&'static str]),
}
