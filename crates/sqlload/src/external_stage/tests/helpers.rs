use crate::column_types::models::ColumnType;
use crate::external_stage::_helpers::create_stage::create_stage;
use crate::external_stage::_helpers::pick_route::pick_route;
use crate::external_stage::main::stage_lookup::stage_lookup;
use crate::external_stage::main::stage_reference::stage_reference;
use crate::external_stage::models::{CopyColumn, CopyValue, StepFailure};
use crate::file_store::models::StoredFile;
use crate::files_source::models::DataFile;
use crate::http::errors::ApiError;
use crate::snowflake_destination::models::{ExternalStage, StageObject, StageRoute};
use crate::store_paths::models::{Location, StoreKind};
use crate::write_sql::models::TableName;

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
            },
        ),
        column(
            "name",
            ColumnType::Text,
            CopyValue::File {
                file_type: Some(ColumnType::Text),
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

/// The direct copies of the case's files from `s3://lake/raw/shop/orders/` (or `source`) into `shop__orders`: how many statements, and the first.
pub(crate) fn direct_sql(
    files: &[(String, &str)],
    columns: &[crate::external_stage::models::CopyColumn],
    source: &Location,
) -> Result<(usize, String), String> {
    let files: Vec<DataFile> = files.iter().map(|(key, day)| data_file(key, day)).collect();
    let stage = stage();
    let batches = crate::external_stage::main::direct_copies::direct_copies(
        &crate::external_stage::models::DirectCopy {
            target: &orders(),
            stage: "@\"ANALYTICS\".\"RAW\".\"_SQLLOAD_STAGE\"",
            stage_url: &stage.location,
            source,
            files: &files,
            columns,
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
    use crate::column_types::models::Warehouse;
    use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};
    use crate::write_sql::main::commit_statements::commit_statements;
    use crate::write_sql::models::{WriteMode, WriteStatementsRequest};
    let columns = vec![DesiredColumn {
        name: "name".to_owned(),
        column_type: ColumnType::Text,
        origin: ColumnOrigin::Source,
    }];
    let staging = TableName {
        table: "staging".to_owned(),
        ..orders()
    };
    let statements = commit_statements(
        &WriteStatementsRequest {
            warehouse: Warehouse::Snowflake,
            table: &orders(),
            columns: &columns,
            mode: &WriteMode::Append,
            hashed: &["name".to_owned()],
        },
        &staging,
        (None, true),
    );
    statements.join(";")
}

/// What `pick_route` says for steps that found `outcomes` (`None` a usable route, `Some((true, reason))` an unusable step, `Some((false, reason))` a failed check): the route and those passed over, or the failure.
pub(crate) fn picked(
    forced: bool,
    outcomes: &[(&str, Option<(bool, &str)>)],
) -> Result<(String, Vec<String>), String> {
    let steps: Vec<(String, Result<StageRoute, StepFailure>)> = outcomes
        .iter()
        .map(|(step, problem)| {
            let outcome = problem.map_or(Ok(StageRoute::User), |(unusable, reason)| {
                Err([
                    StepFailure::Failed(reason.to_owned()),
                    StepFailure::Unusable(reason.to_owned()),
                ][usize::from(unusable)]
                .clone())
            });
            ((*step).to_owned(), outcome)
        })
        .collect();
    let choice = pick_route(forced, steps)?;
    Ok((format!("{:?}", choice.route), choice.passed_over))
}

/// A status error with this HTTP status and service code.
pub(crate) fn api_status(status: u16, code: &str) -> ApiError {
    ApiError::Status {
        status,
        code: code.to_owned(),
        message: "m".to_owned(),
    }
}
