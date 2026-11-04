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
    /// Types pinned over the inferred ones, by column name.
    pub(crate) pinned: &'static [(&'static str, ColumnType)],
    /// Every row read, as `|`-separated values (`NULL` for NULL) without the source-file columns, one per line; or the error text.
    pub(crate) expected_rows: Result<&'static str, &'static str>,
}
