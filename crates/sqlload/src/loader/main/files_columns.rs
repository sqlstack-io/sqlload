//! A files table's columns for `contract diff`: the newest load's, and with every footer each file's own.

use crate::adapters::files::main::read_table_state::read_table_state;
use crate::adapters::files::models::{FilesDestination, TablePath};
use crate::column_types::models::ColumnType;
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::constants::CONCURRENT_FOOTER_READS;
use crate::files_source::main::footer_columns::footer_columns;
use crate::files_source::main::read_footers::read_footers;
use crate::loader::_helpers::destination::{live_schema, table_folder};
use crate::loader::_helpers::python::with_live_columns;
use crate::loader::_helpers::table_plan::{check_output, desired_columns, read_columns};
use crate::loader::errors::LoaderError;
use crate::loader::models::{FileSchema, FilesColumns, TableSpec};
use crate::schema_plan::models::LiveColumn;

/// `spec`'s folder in `destination` and the newest load's schema in its newest partition, as `plan` reads it, again with the live columns for a Python table as `plan` does (capped by `max_footer_reads`); with `all_files`, uncapped, and every data file's own columns too. Reads footers only.
pub async fn files_columns(
    spec: &TableSpec,
    destination: &FilesDestination,
    all_files: bool,
) -> Result<FilesColumns, LoaderError> {
    let (path, cursor_type) = match table_layout(spec, destination) {
        Ok(layout) => layout,
        Err(problems) => {
            return Ok(FilesColumns {
                problems,
                ..FilesColumns::default()
            });
        }
    };
    let folder = open_folder(&path.folder)?;
    let state = read_table_state(&folder, &path, cursor_type.as_ref()).await?;
    let mut columns = FilesColumns {
        folder: folder.location.to_string(),
        problems: state.listed.problems.clone(),
        ..FilesColumns::default()
    };
    let cap = match all_files {
        true => usize::MAX,
        false => destination.max_footer_reads,
    };
    match live_schema(&folder, &state.listed, cap).await? {
        Ok(live) => columns.live = live.live,
        Err(problems) => columns.problems.extend(problems),
    }
    if cursor_type.is_none()
        && let Some(augmented) = with_live_columns(spec, columns.live.as_deref())
    {
        return Box::pin(files_columns(&augmented, destination, all_files)).await;
    }
    if all_files {
        let every: Vec<StoredFile> = state
            .listed
            .partitions
            .iter()
            .flat_map(|partition| partition.files.iter().cloned())
            .collect();
        columns.files = file_schemas(&folder, &every).await?;
    }
    Ok(columns)
}

/// The table's folder and cursor type, as `plan` works them out, or why it has none.
fn table_layout(
    spec: &TableSpec,
    destination: &FilesDestination,
) -> Result<(TablePath, Option<ColumnType>), Vec<String>> {
    let path = table_folder(spec, destination).map_err(|problem| vec![problem])?;
    let desired = desired_columns(spec, &read_columns(spec)?);
    let (cursor_type, problems) = check_output(spec, &path, &desired);
    match problems.is_empty() {
        true => Ok((path, cursor_type)),
        false => Err(problems),
    }
}

/// Each file's key and columns, reading a few footers at a time and keeping only their columns.
async fn file_schemas(
    folder: &OpenFolder,
    files: &[StoredFile],
) -> Result<Vec<FileSchema>, LoaderError> {
    let mut schemas: Vec<FileSchema> = Vec::with_capacity(files.len());
    for chunk in files.chunks(CONCURRENT_FOOTER_READS) {
        for footer in read_footers(folder, chunk).await? {
            let (columns, _) = footer_columns(std::slice::from_ref(&footer));
            let columns: Vec<LiveColumn> = columns
                .into_iter()
                .map(|column| LiveColumn {
                    name: column.name,
                    column_type: column.column_type.unwrap_or_else(ColumnType::Native),
                    fraction_digits: None,
                })
                .collect();
            schemas.push(FileSchema {
                key: footer.relative_key,
                columns,
            });
        }
    }
    Ok(schemas)
}
