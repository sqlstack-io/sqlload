use crate::external_stage::main::direct_problem::direct_problem;
use crate::external_stage::models::{CopyColumn, CopyValue, DirectCheck};
use crate::files_source::constants::{SOURCE_FILE_COLUMN, SOURCE_VERSION_COLUMN};
use crate::files_source::models::{FilesRead, SourceFormat};
use crate::loader::models::{Destination, TableSpec};
use crate::routing::main::plan_route::plan_route;
use crate::routing::models::{
    FileFormat, FileSet, ReaderOffer, Route, RouteOverrides, Step, Store, WriterAccepts,
};
use crate::snowflake_destination::constants::USER_STAGE;
use crate::snowflake_destination::models::{ExternalStage, LoadStage};
use crate::store_paths::models::{Location, StoreKind};

/// What a destination can take: a Snowflake external stage is both where it reads files from and where SQLLoad stages Parquet; otherwise its user stage.
fn writer_accepts(destination: &Destination) -> WriterAccepts {
    match destination {
        #[cfg(feature = "duckdb")]
        Destination::DuckDb(_) => WriterAccepts {
            arrow: true,
            ..WriterAccepts::default()
        },
        Destination::Files(_) => WriterAccepts {
            arrow: true,
            ..WriterAccepts::default()
        },
        Destination::Snowflake(snowflake) => {
            let stage = match &snowflake.stage {
                LoadStage::Internal => Store::InternalStage(format!("@{USER_STAGE}")),
                LoadStage::External(stage) => Store::ObjectStore(stage.location.to_string()),
            };
            WriterAccepts {
                arrow: false,
                pulls_from: vec![stage.clone()],
                pull_formats: vec![FileFormat::Parquet],
                staging: vec![stage],
            }
        }
    }
}

/// The store `location` is in, as routes name it.
fn store_of(location: &Location) -> Store {
    match location.store {
        StoreKind::S3 { .. } => Store::ObjectStore(location.to_string()),
        StoreKind::Filesystem { .. } => Store::Local(location.to_string()),
    }
}

/// What a table's source offers: rows as Arrow, plus the files themselves for a files source, with why the destination must not load them itself when that is the case.
fn reader_offer(
    spec: &TableSpec,
    read: Option<&FilesRead>,
    destination: &Destination,
) -> ReaderOffer {
    let existing_files = spec.files.as_ref().map(|files| FileSet {
        store: store_of(&files.location),
        format: match files.encoding.format {
            SourceFormat::Parquet => FileFormat::Parquet,
            SourceFormat::Ndjson | SourceFormat::Json => FileFormat::Json,
            SourceFormat::Csv => FileFormat::Csv,
        },
    });
    let direct_blocked = match (destination, read) {
        (Destination::Snowflake(snowflake), Some(read)) => match &snowflake.stage {
            LoadStage::External(stage) => direct_check(spec, read, stage),
            LoadStage::Internal => None,
        },
        _ => None,
    };
    ReaderOffer {
        arrow: true,
        existing_files,
        unload_to: Vec::new(),
        estimated_bytes: match u64::try_from(spec.source_table.estimated_bytes) {
            Ok(bytes) => Some(bytes),
            Err(_) => None,
        },
        direct_blocked,
    }
}

/// Why Snowflake must not `COPY` this files table's files straight from `stage`.
fn direct_check(spec: &TableSpec, read: &FilesRead, stage: &ExternalStage) -> Option<String> {
    let files = spec.files.as_ref()?;
    let masks: Vec<(String, crate::project::models::MaskMethod)> = spec
        .masked
        .columns
        .iter()
        .map(|(name, method)| (name.clone(), *method))
        .collect();
    direct_problem(&DirectCheck {
        stage_url: &stage.location,
        source: &files.location,
        format: files.encoding.format,
        files: &read.files,
        columns: &copy_columns(spec, read),
        masks: &masks,
    })
}

/// The columns a direct `COPY` of a files table fills, in the order the files source reads them, each from the files, its path, or the file's name and version.
pub(crate) fn copy_columns(spec: &TableSpec, read: &FilesRead) -> Vec<CopyColumn> {
    let files = spec.files.as_ref();
    read.columns
        .iter()
        .map(|column| {
            let partition = files.is_some_and(|files| {
                files
                    .partition_types
                    .iter()
                    .any(|(name, _)| *name == column.name)
            });
            let value = match column.name.as_str() {
                SOURCE_FILE_COLUMN => CopyValue::SourceFile,
                SOURCE_VERSION_COLUMN => CopyValue::SourceVersion,
                _ if partition => CopyValue::Partition,
                name => CopyValue::File {
                    file_type: files.and_then(|files| {
                        files
                            .table
                            .columns
                            .iter()
                            .find(|found| found.name == name)
                            .and_then(|found| found.column_type.clone().ok())
                    }),
                },
            };
            CopyColumn {
                name: column.name.clone(),
                column_type: column.column_type.clone(),
                value,
            }
        })
        .collect()
}

/// Whether `route` has the destination load a files source's own files.
pub(crate) fn is_direct(route: Option<&Route>) -> bool {
    route.is_some_and(|route| {
        matches!(route.steps.first(), Some(Step::UseFiles(_)))
            && matches!(route.steps.last(), Some(Step::DestinationPulls(_)))
    })
}

/// The route a table's data takes into `destination`; `read` is a files table's planned read.
pub(crate) fn table_route(
    spec: &TableSpec,
    destination: &Destination,
    read: Option<&FilesRead>,
) -> Result<Route, String> {
    let reader = reader_offer(spec, read, destination);
    plan_route(
        &reader,
        &writer_accepts(destination),
        RouteOverrides::default(),
    )
    .map_err(|error| error.to_string())
}
