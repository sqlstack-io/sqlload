use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, TimestampMicrosecondArray};

use crate::file_store::models::StoredFile;
use crate::files_destination::models::{PartitionBy, PartitionFolder, TablePath};
use crate::store_paths::models::{Location, StoreKind};

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
