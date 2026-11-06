//! A files table's columns for `contract diff`: the newest load's, and with every footer each file's own.

use crate::adapters::files::main::read_table_state::read_table_state;
use crate::adapters::files::models::FilesDestination;
use crate::column_types::models::ColumnType;
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::models::StoredFile;
use crate::files_source::main::footer_columns::footer_columns;
use crate::files_source::main::read_footers::read_footers;
use crate::files_source::models::Footer;
use crate::loader::_helpers::destination::{live_schema, table_folder};
use crate::loader::errors::LoaderError;
use crate::loader::models::{FilesColumns, TableSpec};
use crate::schema_plan::models::LiveColumn;

/// `spec`'s folder in `destination` and the newest load's schema as `plan` reads it (capped by `max_footer_reads`); with `all_files`, also every data file's own columns, reading every footer whatever the cap. Reads footers only.
pub async fn files_columns(
    spec: &TableSpec,
    destination: &FilesDestination,
    all_files: bool,
) -> Result<FilesColumns, LoaderError> {
    let path = match table_folder(spec, destination) {
        Ok(path) => path,
        Err(problem) => {
            return Ok(FilesColumns {
                problems: vec![problem],
                ..FilesColumns::default()
            });
        }
    };
    let folder = open_folder(&path.folder)?;
    let state = read_table_state(&folder, &path, None).await?;
    let mut columns = FilesColumns {
        folder: folder.location.to_string(),
        problems: state.listed.problems.clone(),
        ..FilesColumns::default()
    };
    match live_schema(&folder, &state.listed, destination.max_footer_reads).await? {
        Ok(live) => columns.live = live.live,
        Err(problems) => columns.problems.extend(problems),
    }
    if all_files {
        let every: Vec<StoredFile> = state
            .listed
            .partitions
            .iter()
            .flat_map(|partition| partition.files.iter().cloned())
            .collect();
        let footers = read_footers(&folder, &every).await?;
        columns.files = footers.iter().map(file_columns).collect();
    }
    Ok(columns)
}

/// One file's key and columns, from its footer.
fn file_columns(footer: &Footer) -> (String, Vec<LiveColumn>) {
    let (columns, _) = footer_columns(std::slice::from_ref(footer));
    let live: Vec<LiveColumn> = columns
        .into_iter()
        .map(|column| LiveColumn {
            name: column.name,
            column_type: column.column_type.unwrap_or_else(ColumnType::Native),
            fraction_digits: None,
        })
        .collect();
    (footer.relative_key.clone(), live)
}
