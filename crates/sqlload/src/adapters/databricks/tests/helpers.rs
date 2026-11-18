use crate::adapters::contract::models::TableName;
use crate::column_types::models::ColumnType;
use crate::schema_plan::models::{ColumnChange, ColumnOrigin, DesiredColumn, LiveColumn};

/// `main.raw.orders`.
pub(crate) fn table() -> TableName {
    TableName {
        database: Some("main".to_owned()),
        schema: "raw".to_owned(),
        table: "orders".to_owned(),
    }
}

/// A live column.
pub(crate) fn live(name: &str, column_type: ColumnType) -> LiveColumn {
    LiveColumn {
        name: name.to_owned(),
        column_type,
        fraction_digits: None,
    }
}

/// A column added with its DDL type.
pub(crate) fn added(name: &str, column_type: ColumnType, sql_type: &str) -> ColumnChange {
    ColumnChange::Add {
        column: DesiredColumn {
            name: name.to_owned(),
            column_type,
            origin: ColumnOrigin::Source,
        },
        sql_type: sql_type.to_owned(),
    }
}

/// A listed load folder `/v/sqlload/<name>` with one batch `00001` holding one file per time, `part-<n>`.
pub(crate) fn listed_load(
    name: &str,
    times: &[Option<i64>],
) -> crate::adapters::databricks::models::ListedLoad {
    let folder = format!("/v/sqlload/{name}/");
    crate::adapters::databricks::models::ListedLoad {
        batches: vec![format!("{folder}00001/")],
        files: times
            .iter()
            .enumerate()
            .map(
                |(index, time)| crate::databricks_api::models::DirectoryEntry {
                    path: format!("{folder}00001/part-{index}"),
                    is_directory: false,
                    last_modified_ms: *time,
                },
            )
            .collect(),
        folder,
    }
}

/// The `COPY INTO` of two Parquet files under `s3://lake/orders/` (one in the partition folder `day=1/`, holding `2026-11-17`; one, whose name holds a quote and a `+`, with no partition value), batch 2 of load L1: into `main.raw.orders` itself, or into `main.raw.orders_stage` when `staged`. The files hold `id` as `ID`; straight into the table a column `gone` no file holds is written, into the staging table a JSON column `payload` the files hold as `Payload`.
pub(crate) fn s3_copy_sql(staged: bool) -> String {
    use crate::adapters::contract::models::WriteTarget;
    use crate::adapters::contract::types::SqlAdapter;
    use crate::adapters::databricks::_helpers::write::s3_copy::{
        S3Batch, S3File, s3_copy_statement,
    };
    use crate::adapters::databricks::models::DatabricksSql;
    use crate::files_source::constants::{SOURCE_FILE_COLUMN, SOURCE_ROW_COLUMN};
    use crate::load_naming::models::Compat;

    let column = |name: &str, column_type: ColumnType| DesiredColumn {
        name: name.to_owned(),
        column_type,
        origin: ColumnOrigin::Source,
    };
    let last = [
        column("gone", ColumnType::Text),
        column("payload", ColumnType::Json),
    ];
    let columns = vec![
        column("id", ColumnType::Int),
        column("day", ColumnType::Date),
        column(SOURCE_FILE_COLUMN, ColumnType::Text),
        column(SOURCE_ROW_COLUMN, ColumnType::Int),
        last[usize::from(staged)].clone(),
        column("_sqlload_load_id", ColumnType::Text),
        column("_sqlload_loaded_at", ColumnType::TimestampTz),
    ];
    let staging = TableName {
        table: "orders_stage".to_owned(),
        ..table()
    };
    let target = WriteTarget {
        table: &table(),
        staging: Some(&staging).filter(|_| staged),
        columns: &columns,
        write_columns: &columns,
        compat: Compat::Sqlload,
        load_id: "L1",
        batch: 2,
        direct: !staged,
        builds: false,
        constants: &[],
    };
    let file = |key: &str, day: Option<&str>| S3File {
        key: key.to_owned(),
        constants: vec![
            ("day".to_owned(), day.map(str::to_owned)),
            (
                SOURCE_FILE_COLUMN.to_owned(),
                Some(format!("s3://lake/orders/{key}")),
            ),
        ],
    };
    let into = DatabricksSql.render_qualified_name(target.staging.unwrap_or(target.table));
    s3_copy_statement(
        (&into, &columns),
        &target,
        &S3Batch {
            folder: "s3://lake/orders/".to_owned(),
            files: vec![
                file("day=1/a b.parquet", Some("2026-11-17")),
                file("o'brien+x.parquet", None),
            ],
            in_files: vec![
                ("id".to_owned(), "ID".to_owned()),
                ("payload".to_owned(), "Payload".to_owned()),
            ],
        },
        "2026-11-18T00:00:00Z",
    )
}
