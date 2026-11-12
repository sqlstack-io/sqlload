use std::sync::Arc;

use arrow_array::{
    ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::adapters::files::_helpers::write::data_files::{add_rows, finish_file, new_file};
use crate::column_types::models::ColumnType;
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::models::{OpenFolder, StoredFile};
use bytes::Bytes;

use crate::adapters::contract::models::WriteMode;
use crate::adapters::files::errors::FilesDestinationError;
use crate::adapters::files::main::mark_pending::mark_pending;
use crate::adapters::files::main::read_table_state::read_table_state;
use crate::adapters::files::main::repair_table::repair_table;
use crate::adapters::files::models::{
    FolderClaim, PartitionBy, PartitionFolder, PendingPhase, PendingRun, TablePath,
};
use crate::adapters::files::tests::test_types::CommittedRepairTestCase;
use crate::adapters::files::tests::test_types::CsvRenderTestCase;
use crate::column_types::constants::ARROW_TYPE_METADATA_KEY;
use crate::file_store::main::list_files::list_files;
use crate::file_store::main::read_file::read_file;
use crate::file_store::main::write_file::write_file;
use crate::files_source::models::Footer;
use crate::project::models::{CsvOutput, FilesFormat, OutputEncoding};
use crate::schema_plan::main::metadata_columns::metadata_columns;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};
use crate::store_paths::models::{Location, StoreKind};

/// 2026-11-04 00:00:00.000001 UTC, in microseconds.
const NOV_4_PLUS_1_US: i64 = 1_793_750_400_000_001;
/// What `watch_folder` records when the folder is missing.
pub(crate) const NO_FOLDER: &str = "<no folder>";
/// What a reader may also see while a folder is swapped in: nothing on Windows, which cannot swap two folders in one rename, so the old one is moved aside first.
pub(crate) const MID_SWAP: &[&str] = match cfg!(windows) {
    true => &[NO_FOLDER],
    false => &[],
};
/// How the data files a load writes are named.
const PART_NAME_START: &str = "part-";

/// An S3 bucket in eu-west-2.
pub(crate) fn bucket() -> StoreKind {
    StoreKind::S3 {
        bucket: "lake-bucket".to_owned(),
        region: Some("eu-west-2".to_owned()),
        endpoint: None,
        profile: None,
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
        member: None,
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
        source: None,
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
        profile: None,
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
        values: Vec::new(),
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
    let encoding = OutputEncoding::default();
    let file = add_rows(new_file(&schema, &encoding)?, &batch, &encoding)?;
    let bytes = finish_file(file)?;
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
    columns.extend(metadata_columns(
        crate::load_naming::models::Naming::Sqlload,
    ));
    columns
}

/// Write the case's files, mark a committed load replacing `replaced` with a run record, repair the table; the files left and the record object's text.
pub(crate) fn repair_committed(
    test_case: &CommittedRepairTestCase,
) -> Result<(Vec<String>, String), String> {
    let (folder, root) = scratch_folder()?;
    let run = PendingRun {
        prefix: format!("{}_records/", folder.location.prefix),
        name: "20260101T000000Z-load.json".to_owned(),
        json: test_case.expected_record.to_owned(),
    };
    let records = OpenFolder {
        store: folder.store.clone(),
        location: Location {
            store: folder.location.store.clone(),
            prefix: run.prefix.clone(),
        },
    };
    let replaced: Vec<String> = test_case
        .replaced
        .iter()
        .map(|key| (*key).to_owned())
        .collect();
    let outcome = block_on(async {
        let failed = |error: FilesDestinationError| error.to_string();
        for key in test_case.keys {
            write_file(&folder, key, Bytes::from_static(b"x"))
                .await
                .map_err(|error| error.to_string())?;
        }
        mark_pending(
            &folder,
            "load",
            PendingPhase::Replacing,
            (&replaced, Some(&run)),
        )
        .await
        .map_err(failed)?;
        let state = read_table_state(&folder, &orders_path(None), None)
            .await
            .map_err(failed)?;
        repair_table(&folder, &state.pending)
            .await
            .map_err(failed)?;
        let files = list_files(&folder)
            .await
            .map_err(|error| error.to_string())?;
        let record = read_file(&records, &run.name)
            .await
            .map_err(|error| error.to_string())?;
        Ok::<(Vec<String>, String), String>((
            files.into_iter().map(|file| file.relative_key).collect(),
            String::from_utf8_lossy(&record).into_owned(),
        ))
    });
    let _ = std::fs::remove_dir_all(&root);
    outcome
}

/// The bytes of one CSV file holding `batches` in order, written as `encoding` says.
pub(crate) fn csv_file(
    batches: &[RecordBatch],
    encoding: &OutputEncoding,
) -> Result<Vec<u8>, String> {
    let schema = batches
        .first()
        .map(RecordBatch::schema)
        .ok_or("no batches")?;
    let file = batches
        .iter()
        .try_fold(new_file(&schema, encoding)?, |file, batch| {
            add_rows(file, batch, encoding)
        })?;
    finish_file(file)
}

/// The files under `folder`, hidden ones included, as sorted relative keys.
pub(crate) fn local_files(folder: &std::path::Path) -> Vec<String> {
    let mut keys: Vec<String> = walk(folder)
        .into_iter()
        .filter_map(|path| {
            path.strip_prefix(folder)
                .ok()
                .map(|relative| relative.to_string_lossy().replace('\\', "/"))
        })
        .collect();
    keys.sort();
    keys
}

fn walk(folder: &std::path::Path) -> Vec<std::path::PathBuf> {
    let entries: Vec<std::path::PathBuf> = std::fs::read_dir(folder)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| entry.path())
        .collect();
    let files = entries.iter().filter(|path| !path.is_dir()).cloned();
    let nested = entries
        .iter()
        .filter(|path| path.is_dir())
        .flat_map(|path| walk(path));
    files.chain(nested).collect()
}

/// List `folder`'s data files (named `part-*`) over and over on another thread until `stop` is set; every distinct set of names seen.
pub(crate) fn watch_folder(
    folder: std::path::PathBuf,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> std::thread::JoinHandle<std::collections::BTreeSet<Vec<String>>> {
    std::thread::spawn(move || {
        std::iter::from_fn(|| {
            (!stop.load(std::sync::atomic::Ordering::Relaxed)).then_some(part_names(&folder))
        })
        .collect()
    })
}

/// The names of `folder`'s `part-*` files, sorted; `NO_FOLDER` when there is no folder.
fn part_names(folder: &std::path::Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(folder) else {
        return vec![NO_FOLDER.to_owned()];
    };
    let mut names: Vec<String> = entries
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with(PART_NAME_START))
        .collect();
    names.sort();
    names
}

/// Write `body` to each of `keys` below `root`/`prefix`, creating folders.
pub(crate) fn write_all(
    root: &std::path::Path,
    prefix: &str,
    keys: &[&str],
    body: &str,
) -> Result<(), String> {
    keys.iter().try_for_each(|key| {
        let path = root.join(prefix).join(key);
        std::fs::create_dir_all(path.parent().ok_or("no parent")?)
            .and_then(|()| std::fs::write(&path, body))
            .map_err(|error| error.to_string())
    })
}

/// A nullable column named `name` of `array`'s type.
pub(crate) fn column(name: &str, array: ArrayRef) -> (Field, ArrayRef) {
    (Field::new(name, array.data_type().clone(), true), array)
}

/// A text column tagged as `json`.
pub(crate) fn json_column(name: &str, values: Vec<Option<&str>>) -> (Field, ArrayRef) {
    let field =
        Field::new(name, DataType::Utf8, true).with_metadata(std::collections::HashMap::from([(
            ARROW_TYPE_METADATA_KEY.to_owned(),
            ColumnType::Json.to_string(),
        )]));
    (field, Arc::new(StringArray::from(values)))
}

/// 2026-11-04 00:00:00.000001 UTC and a NULL, as timestamps in `zone`.
pub(crate) fn instants(zone: Option<&str>) -> ArrayRef {
    Arc::new(
        TimestampMicrosecondArray::from(vec![Some(NOV_4_PLUS_1_US), None])
            .with_timezone_opt(zone.map(str::to_owned)),
    )
}

/// Tab-separated, no header, NULL as `NULL`.
pub(crate) fn tab_separated() -> CsvOutput {
    CsvOutput {
        delimiter: '\t',
        header: false,
        quote: '"',
        null_value: "NULL".to_owned(),
    }
}

/// The CSV text of one file holding the case's columns.
pub(crate) fn csv_text(test_case: &CsvRenderTestCase) -> Result<String, String> {
    let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = test_case.columns.iter().cloned().unzip();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|error| error.to_string())?;
    let encoding = OutputEncoding {
        format: FilesFormat::Csv,
        csv: test_case.csv.clone(),
        ..OutputEncoding::default()
    };
    let bytes = csv_file(&[batch], &encoding)?;
    String::from_utf8(bytes).map_err(|error| error.to_string())
}

/// Uncompressed bytes, as they are.
pub(crate) fn plain_bytes(bytes: &[u8]) -> Result<Vec<u8>, String> {
    Ok(bytes.to_vec())
}

/// gzip bytes decompressed.
pub(crate) fn gunzip(bytes: &[u8]) -> Result<Vec<u8>, String> {
    let mut text = Vec::new();
    std::io::Read::read_to_end(&mut flate2::read::GzDecoder::new(bytes), &mut text)
        .map_err(|error| error.to_string())?;
    Ok(text)
}

/// zstd bytes decompressed.
pub(crate) fn unzstd(bytes: &[u8]) -> Result<Vec<u8>, String> {
    zstd::decode_all(bytes).map_err(|error| error.to_string())
}
