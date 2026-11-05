/// A file format SQLLoad reads or writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    Parquet,
    /// Newline-delimited JSON, optionally gzip-compressed.
    Json,
    Csv,
}

impl std::fmt::Display for FileFormat {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Parquet => "parquet",
            Self::Json => "json",
            Self::Csv => "csv",
        })
    }
}

/// Where files live.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Store {
    /// An object store prefix such as `s3://bucket/path/`, `gs://...` or `az://...`.
    ObjectStore(String),
    /// A directory on the machine SQLLoad runs on.
    Local(String),
    /// A warehouse's own internal stage, such as Snowflake `@~` or a named internal stage.
    InternalStage(String),
}

impl Store {
    /// Whether `self` lies under `other` (same kind, and `other` is a prefix of it).
    pub fn is_within(&self, other: &Store) -> bool {
        match (self, other) {
            (Self::ObjectStore(path), Self::ObjectStore(prefix))
            | (Self::Local(path), Self::Local(prefix))
            | (Self::InternalStage(path), Self::InternalStage(prefix)) => {
                path.starts_with(prefix.as_str())
            }
            _ => false,
        }
    }
}

impl std::fmt::Display for Store {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ObjectStore(path) | Self::Local(path) => formatter.write_str(path),
            Self::InternalStage(name) => write!(formatter, "internal stage {name}"),
        }
    }
}

/// Files of one format in one store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSet {
    pub store: Store,
    pub format: FileFormat,
}

/// What a source can produce.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReaderOffer {
    /// It can stream Arrow record batches (a database query, Python `yield`, Kafka).
    pub arrow: bool,
    /// Files that already exist and hold the data.
    pub existing_files: Option<FileSet>,
    /// Stores a warehouse source can unload Parquet into, in order of preference.
    pub unload_to: Vec<Store>,
    /// The data's estimated size, when the source knows it.
    pub estimated_bytes: Option<u64>,
    /// Why the destination must not read `existing_files` itself even where it can reach them (SQLLoad would load them differently).
    pub direct_blocked: Option<String>,
}

/// What a destination can take.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriterAccepts {
    /// It can take Arrow record batches directly (Postgres binary `COPY`, ClickHouse insert, a Parquet file writer).
    pub arrow: bool,
    /// Stores it can read files from itself (`COPY` from a stage, ClickHouse `s3()`, a same-store copy).
    pub pulls_from: Vec<Store>,
    /// Formats it can read when pulling files.
    pub pull_formats: Vec<FileFormat>,
    /// Stores where SQLLoad may write files for it to pull, in order of preference (external first).
    pub staging: Vec<Store>,
}

/// How a warehouse source is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadVia {
    /// Unload above the size threshold (or when the size is unknown), query below it.
    #[default]
    Auto,
    Query,
    Unload,
}

/// Which kind of stage to stage files in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StagePreference {
    /// An external stage (an object store) if one exists, otherwise an internal stage.
    #[default]
    Auto,
    External,
    Internal,
}

/// User choices that override the planner's defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RouteOverrides {
    pub read_via: ReadVia,
    pub stage: StagePreference,
}

/// One step of a route.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// The source streams Arrow batches (a query, Python `yield`, Kafka).
    ReadArrow,
    /// The warehouse source unloads Parquet into a store.
    Unload(Store),
    /// Existing files are listed and used as they are.
    UseFiles(FileSet),
    /// Adapter: files are read into Arrow batches.
    FileReader(FileFormat),
    /// Adapter: Arrow batches are written as Parquet into a store.
    ParquetWriter(Store),
    /// The destination reads the files itself; no data passes through SQLLoad.
    DestinationPulls(FileSet),
    /// The destination takes Arrow batches.
    DestinationTakesArrow,
}

impl std::fmt::Display for Step {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReadArrow => formatter.write_str("read as Arrow"),
            Self::Unload(store) => write!(formatter, "unload Parquet to {store}"),
            Self::UseFiles(files) => {
                write!(formatter, "use {} files in {}", files.format, files.store)
            }
            Self::FileReader(format) => write!(formatter, "read {format} into Arrow"),
            Self::ParquetWriter(store) => write!(formatter, "write Parquet to {store}"),
            Self::DestinationPulls(files) => {
                write!(
                    formatter,
                    "destination loads {} files from {}",
                    files.format, files.store
                )
            }
            Self::DestinationTakesArrow => formatter.write_str("destination takes Arrow"),
        }
    }
}

/// The steps that move one table's data, and why they were chosen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub steps: Vec<Step>,
    pub reason: String,
}
