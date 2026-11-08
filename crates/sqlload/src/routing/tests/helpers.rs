use crate::routing::models::{
    FileFormat, FileSet, ReaderOffer, RouteOverrides, StagePreference, Store, WriterAccepts,
};

pub(crate) const LAKE: &str = "s3://lake/";
pub(crate) const USER_STAGE: &str = "@~";

pub(crate) fn object_store(path: &str) -> Store {
    Store::ObjectStore(path.to_owned())
}

pub(crate) fn user_stage() -> Store {
    Store::InternalStage(USER_STAGE.to_owned())
}

/// A database or Python source: streams Arrow only.
pub(crate) fn streaming_source() -> ReaderOffer {
    ReaderOffer {
        arrow: true,
        ..ReaderOffer::default()
    }
}

/// Existing files of `format` under `path`.
pub(crate) fn files_source(path: &str, format: FileFormat) -> ReaderOffer {
    ReaderOffer {
        existing_files: Some(FileSet {
            store: object_store(path),
            format,
        }),
        ..ReaderOffer::default()
    }
}

/// A warehouse source that can query, and unload to the lake or its user stage.
pub(crate) fn warehouse_source(estimated_bytes: Option<u64>) -> ReaderOffer {
    ReaderOffer {
        arrow: true,
        unload_to: vec![object_store("s3://lake/unload/"), user_stage()],
        estimated_bytes,
        ..ReaderOffer::default()
    }
}

/// A database destination that takes Arrow (Postgres binary `COPY`).
pub(crate) fn database_destination() -> WriterAccepts {
    WriterAccepts {
        arrow: true,
        ..WriterAccepts::default()
    }
}

/// A warehouse destination loading from stages listed in `staging`.
pub(crate) fn warehouse_destination(staging: Vec<Store>) -> WriterAccepts {
    WriterAccepts {
        arrow: false,
        pulls_from: staging.clone(),
        pull_formats: vec![FileFormat::Parquet, FileFormat::Json, FileFormat::Csv],
        staging,
        rows_into: None,
        reads_files_itself: Some("Snowflake"),
        copies_files_to: Some(user_stage()),
    }
}

/// ClickHouse: takes Arrow and reads the lake itself through `s3()`.
pub(crate) fn clickhouse_destination() -> WriterAccepts {
    WriterAccepts {
        arrow: true,
        pulls_from: vec![object_store(LAKE)],
        pull_formats: vec![FileFormat::Parquet],
        staging: Vec::new(),
        rows_into: None,
        reads_files_itself: None,
        copies_files_to: None,
    }
}

/// DuckDB: takes Arrow, and reads Parquet itself from S3 or a local folder.
pub(crate) fn duckdb_destination() -> WriterAccepts {
    WriterAccepts {
        arrow: true,
        pulls_from: vec![object_store("s3://"), Store::Local(String::new())],
        pull_formats: vec![FileFormat::Parquet],
        ..WriterAccepts::default()
    }
}

/// A local directory: writes Parquet from Arrow, downloads from the lake, and fetches from the user stage.
pub(crate) fn local_destination() -> WriterAccepts {
    WriterAccepts {
        arrow: true,
        pulls_from: vec![object_store(LAKE), user_stage()],
        pull_formats: vec![FileFormat::Parquet],
        staging: Vec::new(),
        rows_into: None,
        reads_files_itself: None,
        copies_files_to: None,
    }
}

/// Overrides choosing the internal stage.
pub(crate) fn internal_stage() -> RouteOverrides {
    RouteOverrides {
        stage: StagePreference::Internal,
        ..RouteOverrides::default()
    }
}
