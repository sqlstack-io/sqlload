use crate::adapters::contract::models::TableName;
use crate::adapters::snowflake::_helpers::stage::create_stage::create_stage;
use crate::adapters::snowflake::_helpers::stage::stale_folders::stale_folders;
use crate::adapters::snowflake::main::copy::_copy_staged::copy_staged;
use crate::adapters::snowflake::main::stage::_stage_lookup::stage_lookup;
use crate::adapters::snowflake::main::stage::_stage_reference::stage_reference;
use crate::adapters::snowflake::models::CopyStaged;
use crate::adapters::snowflake::models::{CopyColumn, CopyValue};
use crate::adapters::snowflake::models::{ExternalStage, StageObject};
use crate::adapters::snowflake::tests::test_types::CopyFrom;
use crate::column_types::models::ColumnType;
use crate::file_store::models::StoredFile;
use crate::files_source::models::DataFile;
use crate::files_source::models::StoredRead;
use crate::http::errors::ApiError;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};
use crate::store_paths::models::{Location, StoreKind};
use chrono::{DateTime, Utc};

/// Files under the managed stage on `s3://lake/raw/`, read where they are.
pub(crate) const EXTERNAL: CopyFrom = CopyFrom {
    source: "raw/shop/orders/",
    stage: "@\"ANALYTICS\".\"RAW\".\"_SQLLOAD_STAGE\"",
    folder: "shop/orders/",
    filename_prefix: "raw/shop/orders/",
};

/// Files of `s3://lake/other/` no stage covers, copied unchanged to `@~` for batch 1 of load `L`.
pub(crate) const INTERNAL: CopyFrom = CopyFrom {
    source: "other/",
    stage: "@~",
    folder: "sqlload/L/00001/",
    filename_prefix: "sqlload/L/00001/",
};

/// The `COPY` statements for `files` staged parts of batch 1 of load `load-1`: how many, and the first.
pub(crate) fn copies(files: usize) -> (usize, String) {
    let table = TableName {
        database: Some("analytics".to_owned()),
        schema: "raw".to_owned(),
        table: "shop__orders".to_owned(),
    };
    let names: Vec<String> = (1..=files)
        .map(|part| format!("part-{part:05}.parquet"))
        .collect();
    let statements = copy_staged(&CopyStaged {
        table: &table,
        stage: "@~",
        folder: "sqlload/load-1/00001/",
        names: &names,
        selected: None,
    });
    (
        statements.len(),
        statements.first().cloned().unwrap_or_default(),
    )
}

/// The load folders older than a day before 2026-11-05 10:00 UTC.
pub(crate) fn stale(listed: &[(&str, &str)]) -> Vec<String> {
    let listed: Vec<(String, String)> = listed
        .iter()
        .map(|(name, modified)| ((*name).to_owned(), (*modified).to_owned()))
        .collect();
    let before = DateTime::parse_from_rfc3339("2026-11-04T10:00:00Z")
        .map(|time| time.with_timezone(&Utc))
        .unwrap_or_default();
    stale_folders(&listed, before)
}

/// The `COPY` of one staged file into a table holding JSON, naming the table's columns.
pub(crate) fn selecting_copy(columns: &[(&str, ColumnType)]) -> String {
    let table = TableName {
        database: None,
        schema: "raw".to_owned(),
        table: "orders".to_owned(),
    };
    let selected: Vec<DesiredColumn> = columns
        .iter()
        .map(|(name, column_type)| DesiredColumn {
            name: (*name).to_owned(),
            column_type: column_type.clone(),
            origin: ColumnOrigin::Source,
        })
        .collect();
    copy_staged(&CopyStaged {
        table: &table,
        stage: "@\"RAW\".%\"ORDERS\"",
        folder: "sqlload/l/00001/",
        names: &["part-00001.parquet".to_owned()],
        selected: Some(&selected),
    })
    .concat()
}

/// `s3://lake/<prefix>`.
pub(crate) fn lake(prefix: &str) -> Location {
    Location {
        store: StoreKind::S3 {
            bucket: "lake".to_owned(),
            region: None,
            endpoint: None,
        },
        prefix: prefix.to_owned(),
    }
}

/// The stage on `s3://lake/raw/` SQLLoad creates with integration `lake_int`.
pub(crate) fn stage() -> ExternalStage {
    ExternalStage {
        location: lake("raw/"),
        object: StageObject::Managed {
            integration: "lake_int".to_owned(),
        },
        home: TableName {
            table: String::new(),
            ..orders()
        },
        keep_staged_files: false,
    }
}

/// An existing stage on `s3://lake/raw/`.
pub(crate) fn existing_stage() -> ExternalStage {
    ExternalStage {
        location: lake("raw/"),
        object: StageObject::Existing {
            name: "ops.loading.lake_stage".to_owned(),
        },
        home: TableName {
            table: String::new(),
            ..orders()
        },
        keep_staged_files: false,
    }
}

pub(crate) fn orders() -> TableName {
    TableName {
        database: Some("analytics".to_owned()),
        schema: "raw".to_owned(),
        table: "shop__orders".to_owned(),
    }
}

/// A listed file with an ETag.
pub(crate) fn data_file(key: &str, day: &str) -> DataFile {
    DataFile {
        file: StoredFile {
            path: object_store::path::Path::from(format!("raw/shop/orders/{key}")),
            relative_key: key.to_owned(),
            size: 10,
            etag: Some(format!("\"etag-{day}\"")),
            modified_ms: 0,
        },
        partitions: vec![("day".to_owned(), day.to_owned())],
    }
}

pub(crate) fn column(name: &str, column_type: ColumnType, value: CopyValue) -> CopyColumn {
    CopyColumn {
        name: name.to_owned(),
        column_type,
        value,
    }
}

/// `id` (int) and `name` (text) from the files, `day` from the path, and the source file and version.
pub(crate) fn order_columns() -> Vec<CopyColumn> {
    vec![
        column(
            "id",
            ColumnType::Int,
            CopyValue::File {
                file_type: Some(ColumnType::Int),
                stored: StoredRead::Plain,
                short: false,
            },
        ),
        column(
            "name",
            ColumnType::Text,
            CopyValue::File {
                file_type: Some(ColumnType::Text),
                stored: StoredRead::Plain,
                short: false,
            },
        ),
        column("day", ColumnType::Date, CopyValue::Partition),
        column(
            "_sqlload_source_file",
            ColumnType::Text,
            CopyValue::SourceFile,
        ),
        column(
            "_sqlload_source_version",
            ColumnType::Text,
            CopyValue::SourceVersion,
        ),
    ]
}

pub(crate) fn managed_create() -> String {
    create_stage(&stage()).unwrap_or_default()
}

pub(crate) fn existing_create() -> String {
    create_stage(&existing_stage()).unwrap_or_default()
}

pub(crate) fn managed_lookup() -> String {
    let lookup = stage_lookup(&stage());
    format!("{};\n{}", lookup.query, lookup.name)
}

pub(crate) fn existing_lookup() -> String {
    let lookup = stage_lookup(&existing_stage());
    format!("{};\n{}", lookup.query, lookup.name)
}

pub(crate) fn managed_reference() -> String {
    stage_reference(&stage())
}

/// The direct copies of the case's files from `from` into `shop__orders`: how many statements, and the first.
pub(crate) fn direct_sql(
    files: &[(String, &str)],
    columns: &[crate::adapters::snowflake::models::CopyColumn],
    from: &CopyFrom,
) -> Result<(usize, String), String> {
    let files: Vec<DataFile> = files.iter().map(|(key, day)| data_file(key, day)).collect();
    let source = lake(from.source);
    let batches = crate::adapters::snowflake::main::copy::_direct_copies::direct_copies(
        &crate::adapters::snowflake::models::DirectCopy {
            target: &orders(),
            stage: from.stage,
            folder: from.folder,
            filename_prefix: from.filename_prefix,
            source: &source,
            files: &files,
            columns,
            encoding: &crate::files_source::models::FilesEncoding::default(),
            inferred: &[],
            headers: &std::collections::BTreeMap::new(),
        },
    )?;
    Ok((
        batches.len(),
        batches
            .first()
            .map(|batch| batch.sql.clone())
            .unwrap_or_default(),
    ))
}

/// `count` files named `day=2026-11-04/part-<n>.parquet`, with `padding` extra characters in each name.
pub(crate) fn many_files(count: usize, padding: usize) -> Vec<(String, &'static str)> {
    (0..count)
        .map(|index| {
            (
                format!(
                    "day=2026-11-04/part-{index:05}{}.parquet",
                    "x".repeat(padding)
                ),
                "2026-11-04",
            )
        })
        .collect()
}

/// `value` masked with `hash` by SQLLoad's own reader, the route the direct COPY must match.
pub(crate) fn rust_hash(value: &str) -> Result<String, String> {
    use arrow_array::cast::AsArray;
    let batch = arrow_array::RecordBatch::try_from_iter([(
        "name",
        std::sync::Arc::new(arrow_array::StringArray::from(vec![value])) as arrow_array::ArrayRef,
    )])
    .map_err(|error| error.to_string())?;
    let rules = crate::masking::models::MaskRules {
        columns: [("name".to_owned(), crate::project::models::MaskMethod::Hash)].into(),
        key: None,
    };
    let masked = crate::masking::main::mask_batch::mask_batch(batch, &rules)?;
    Ok(masked.column(0).as_string::<i32>().value(0).to_owned())
}

/// The expression the write applies to a hashed staging column on Snowflake.
pub(crate) fn snowflake_hash_sql() -> String {
    use crate::adapters::contract::models::{WriteMode, WriteStatementsRequest};
    use crate::adapters::contract::types::SqlAdapter;
    use crate::adapters::snowflake::models::SnowflakeSql;
    use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};
    let columns = vec![DesiredColumn {
        name: "name".to_owned(),
        column_type: ColumnType::Text,
        origin: ColumnOrigin::Source,
    }];
    let staging = TableName {
        table: "staging".to_owned(),
        ..orders()
    };
    let statements = SnowflakeSql.render_commit(
        &WriteStatementsRequest {
            table: &orders(),
            columns: &columns,
            mode: &WriteMode::Append,
            hashed: &["name".to_owned()],
            built: None,
        },
        &staging,
        (None, true),
    );
    statements.join(";")
}

/// A status error with this HTTP status and service code.
pub(crate) fn api_status(status: u16, code: &str) -> ApiError {
    ApiError::Status {
        status,
        code: code.to_owned(),
        message: "m".to_owned(),
    }
}
