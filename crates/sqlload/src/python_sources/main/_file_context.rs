//! The `ctx` a files table's `transform` gets with one file's rows.

use crate::files_source::models::{DataFile, FilesEncoding, SourceFormat};
use crate::python_sources::models::TransformContext;
use crate::time_text::main::iso_utc::iso_utc;

/// Where `data` came from, for table `table`: its name as `_sqlload_source_file` has it (`file_name`), when it was modified, and the sheet, range and rows skipped its format reads by.
pub(in crate::python_sources) fn file_context(
    table: &str,
    (file_name, data): (&str, &DataFile),
    encoding: &FilesEncoding,
) -> TransformContext {
    let (sheet, range, skip_rows) = match encoding.format {
        SourceFormat::Xlsx => (
            encoding.xlsx.sheet.clone(),
            encoding.xlsx.range.clone(),
            Some(encoding.xlsx.skip_rows),
        ),
        SourceFormat::Csv => (
            None,
            None,
            Some(u32::try_from(encoding.csv.skip_rows).unwrap_or(u32::MAX)),
        ),
        _ => (None, None, None),
    };
    TransformContext {
        unit: "file",
        table: table.to_owned(),
        path: Some(file_name.to_owned()),
        modified_at: Some(iso_utc(data.file.modified_ms)),
        sheet,
        range,
        skip_rows,
        ..TransformContext::default()
    }
}
