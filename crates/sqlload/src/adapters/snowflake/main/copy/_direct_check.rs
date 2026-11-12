//! Whether Snowflake may `COPY` a files table's files itself.

use crate::adapters::snowflake::_helpers::dialect::text_values::snowflake_encoding;
use crate::adapters::snowflake::_helpers::write::copy_columns::copy_columns;
use crate::adapters::snowflake::main::copy::_direct_problem::direct_problem;
use crate::adapters::snowflake::models::{DirectCheck, ExternalStage};
use crate::files_source::models::{DataFile, FilesRead, SourceFormat};
use crate::loader::models::TableSpec;
use crate::project::models::MaskMethod;

/// Why Snowflake cannot `COPY` these of a files table's files exactly, straight from `stage` (or copied unchanged to an internal stage when `None`); `refused` is what a batch's own footers refuse, beyond what plan sampled.
pub(in crate::adapters) fn direct_check(
    spec: &TableSpec,
    (read, files, refused): (&FilesRead, &[DataFile], &[(String, String)]),
    stage: Option<&ExternalStage>,
) -> Option<String> {
    let source = spec.files.as_ref()?;
    let label = &source.encoding.csv.encoding;
    if source.encoding.format == SourceFormat::Csv && snowflake_encoding(label).is_none() {
        return Some(format!(
            "csv encoding {label:?} has no Snowflake COPY encoding SQLLoad reads the same way; Snowflake reads CSV in utf-8, latin-1, windows-1250 to windows-1256, iso-8859-2, -5, -6, -7 or -15, koi8-r, utf-16le or utf-16be"
        ));
    }
    let mut all_refused: Vec<(String, String)> = refused.to_vec();
    for column in &source.table.columns {
        if let Some(arrow_type) = &column.direct_refusal {
            all_refused.push((column.name.clone(), arrow_type.clone()));
        }
    }
    let masks: Vec<(String, MaskMethod)> = spec
        .masked
        .columns
        .iter()
        .map(|(name, method)| (name.clone(), *method))
        .collect();
    direct_problem(&DirectCheck {
        stage_url: stage.map(|stage| &stage.location),
        source: &source.location,
        format: source.encoding.format,
        whole: source.encoding.record.is_some(),
        files,
        columns: &copy_columns(spec, read),
        refused: &all_refused,
        masks: &masks,
    })
}
