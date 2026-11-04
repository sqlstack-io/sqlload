use std::sync::Arc;

use arrow_array::{Date32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::column_types::models::ColumnType;
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_destination::_helpers::parquet_files::new_file;
use crate::files_destination::models::{FolderClaim, PartitionBy, PartitionFolder, TablePath};
use crate::files_source::models::Footer;
use crate::schema_plan::main::metadata_columns::metadata_columns;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};
use crate::store_paths::models::{Location, StoreKind};
use crate::write_sql::models::WriteMode;

/// An S3 bucket in eu-west-2.
pub(crate) fn bucket() -> StoreKind {
    StoreKind::S3 {
        bucket: "lake-bucket".to_owned(),
        region: Some("eu-west-2".to_owned()),
        endpoint: None,
    }
}

/// A listed file of one byte.
pub(crate) fn stored(key: &str) -> StoredFile {
    StoredFile {
        path: object_store::path::Path::from(key),
        relative_key: key.to_owned(),
        size: 1,
        etag: None,
        modified_ms: 0,
    }
}

/// The table `shop/orders/` with folders `<prefix>{cursor...}` below it.
pub(crate) fn orders_path(partition: Option<(&str, PartitionBy)>) -> TablePath {
    TablePath {
        folder: Location {
            store: bucket(),
            prefix: "shop/orders/".to_owned(),
        },
        partition: partition.map(|(prefix, by)| PartitionFolder {
            prefix: prefix.to_owned(),
            suffix: String::new(),
            by,
        }),
    }
}

/// Rows with an `id` and a UTC `updated_at` cursor.
pub(crate) fn instants_batch(instants: &[Option<i64>]) -> Result<RecordBatch, String> {
    let ids: Vec<i64> = (0..instants.len())
        .map(|index| i64::try_from(index).unwrap_or_default())
        .collect();
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from(ids)) as arrow_array::ArrayRef,
        ),
        (
            "updated_at",
            Arc::new(TimestampMicrosecondArray::from(instants.to_vec()).with_timezone("UTC")),
        ),
    ])
    .map_err(|error| error.to_string())
}

/// `prefix` in `store`.
pub(crate) fn location(store: StoreKind, prefix: &str) -> Location {
    Location {
        store,
        prefix: prefix.to_owned(),
    }
}

/// A table claiming `prefix` in `store`, from `template` and source schema `schema`.
pub(crate) fn claim(name: &str, store: StoreKind, prefix: &str, schema: &str) -> FolderClaim {
    FolderClaim {
        id: name.to_owned(),
        name: name.to_owned(),
        folder: location(store, prefix),
        template: "{source}/{table}/".to_owned(),
        schema: schema.to_owned(),
    }
}

/// A listed file modified at `modified_ms`.
pub(crate) fn stored_at(key: &str, modified_ms: i64) -> StoredFile {
    StoredFile {
        modified_ms,
        ..stored(key)
    }
}

/// An S3 bucket reached through another endpoint and region.
pub(crate) fn bucket_elsewhere() -> StoreKind {
    StoreKind::S3 {
        bucket: "lake-bucket".to_owned(),
        region: Some("us-east-1".to_owned()),
        endpoint: Some("http://127.0.0.1:9000".to_owned()),
    }
}

/// A directory store at `root`.
pub(crate) fn directory(root: &str) -> StoreKind {
    StoreKind::Filesystem {
        root: root.to_owned(),
    }
}

/// `delete_insert` on `day` over `from <= day < to`.
pub(crate) fn delete_insert(from: Option<&str>, to: Option<&str>) -> WriteMode {
    WriteMode::DeleteInsert {
        cursor_column: "day".to_owned(),
        from: from.map(str::to_owned),
        to: to.map(str::to_owned),
    }
}

/// The footer of a Parquet file the destination writes, holding text column `code` with `values`.
pub(crate) fn text_footer(values: &[&str]) -> Result<Footer, String> {
    let schema = Arc::new(Schema::new(vec![Field::new("code", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(values.to_vec())) as arrow_array::ArrayRef],
    )
    .map_err(|error| error.to_string())?;
    let mut file = new_file(&schema)?;
    file.writer
        .write(&batch)
        .map_err(|error| error.to_string())?;
    let bytes = file
        .writer
        .into_inner()
        .map_err(|error| error.to_string())?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
        .map_err(|error| error.to_string())?;
    Ok(Footer {
        relative_key: "part-a-0.parquet".to_owned(),
        schema: builder.schema().clone(),
        metadata: builder.metadata().clone(),
    })
}

/// Run `body` to completion on a fresh runtime.
pub(crate) fn block_on<T>(
    body: impl std::future::Future<Output = Result<T, String>>,
) -> Result<T, String> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?
        .block_on(body)
}

/// A fresh directory store's table folder `orders/`, and the directory to remove afterwards.
pub(crate) fn scratch_folder() -> Result<(OpenFolder, std::path::PathBuf), String> {
    let root = std::env::temp_dir().join(format!("sqlload-unit-{}", uuid::Uuid::new_v4()));
    let folder = open_folder(&Location {
        store: StoreKind::Filesystem {
            root: root.display().to_string(),
        },
        prefix: "orders/".to_owned(),
    })
    .map_err(|error| error.to_string())?;
    Ok((folder, root))
}

/// A batch of `count` rows, each on its own day from 2026-01-01.
pub(crate) fn days_batch(count: usize) -> Result<RecordBatch, String> {
    let ids: Vec<i64> = (0..count)
        .map(|index| i64::try_from(index).unwrap_or_default())
        .collect();
    let days: Vec<i32> = (0..count)
        .map(|index| 20_454 + i32::try_from(index).unwrap_or_default())
        .collect();
    RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from(ids)) as arrow_array::ArrayRef,
        ),
        ("day", Arc::new(Date32Array::from(days))),
    ])
    .map_err(|error| error.to_string())
}

/// `id int`, `day date`, then the metadata columns.
pub(crate) fn day_columns() -> Vec<DesiredColumn> {
    let mut columns = vec![
        DesiredColumn {
            name: "id".to_owned(),
            column_type: ColumnType::Int,
            origin: ColumnOrigin::Source,
        },
        DesiredColumn {
            name: "day".to_owned(),
            column_type: ColumnType::Date,
            origin: ColumnOrigin::Source,
        },
    ];
    columns.extend(metadata_columns());
    columns
}
