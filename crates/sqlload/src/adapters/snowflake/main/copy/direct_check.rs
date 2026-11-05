//! Whether Snowflake may `COPY` a files table's files itself.

use crate::adapters::snowflake::_helpers::write::copy_columns::copy_columns;
use crate::adapters::snowflake::main::copy::_direct_problem::direct_problem;
use crate::adapters::snowflake::models::{DirectCheck, ExternalStage};
use crate::files_source::models::{DataFile, FilesRead};
use crate::loader::models::TableSpec;
use crate::project::models::MaskMethod;

/// Why Snowflake must not `COPY` these of a files table's files straight from `stage`; `refused` is what a batch's own footers refuse, beyond what plan sampled.
pub fn direct_check(
    spec: &TableSpec,
    (read, files, refused): (&FilesRead, &[DataFile], &[(String, String)]),
    stage: &ExternalStage,
) -> Option<String> {
    let source = spec.files.as_ref()?;
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
        stage_url: &stage.location,
        source: &source.location,
        format: source.encoding.format,
        files,
        columns: &copy_columns(spec, read),
        refused: &all_refused,
        masks: &masks,
        shaped_by_loader: spec.shaped_by_loader,
    })
}
