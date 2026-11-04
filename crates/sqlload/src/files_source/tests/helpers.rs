use std::io::Write;

use arrow_array::RecordBatch;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use bytes::Bytes;
use flate2::Compression as GzipLevel;
use flate2::write::GzEncoder;

use crate::column_types::models::ColumnType;
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::main::write_file::write_file;
use crate::file_store::models::OpenFolder;
use crate::files_source::main::discover_files::discover_files;
use crate::files_source::main::read_files::read_files;
use crate::files_source::models::{
    Compression, CsvOptions, FilesEncoding, FilesRead, FilesTable, SourceFormat,
};
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};
use crate::store_paths::models::{Location, StoreKind};

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

/// `format` files, compressed as their names say.
pub(crate) fn encoding(format: SourceFormat) -> FilesEncoding {
    FilesEncoding {
        format,
        compression: Compression::Auto,
        csv: CsvOptions::default(),
    }
}

/// CSV files with `csv` options.
pub(crate) fn csv_encoding(csv: CsvOptions) -> FilesEncoding {
    FilesEncoding {
        format: SourceFormat::Csv,
        compression: Compression::Auto,
        csv,
    }
}

/// Turns a test file's text into its bytes.
type Encoder = fn(&str) -> Result<Vec<u8>, String>;

/// How a test file is written, by key suffix: the first suffix that matches wins.
const ENCODERS: &[(&str, Encoder)] = &[(".gz", gzip), ("", plain)];

fn plain(text: &str) -> Result<Vec<u8>, String> {
    Ok(text.as_bytes().to_vec())
}

fn gzip(text: &str) -> Result<Vec<u8>, String> {
    let mut encoder = GzEncoder::new(Vec::new(), GzipLevel::default());
    encoder
        .write_all(text.as_bytes())
        .map_err(|error| error.to_string())?;
    encoder.finish().map_err(|error| error.to_string())
}

/// `text` gzip-compressed when the key ends in `.gz`, as written otherwise.
fn file_bytes(key: &str, text: &str) -> Result<Bytes, String> {
    let encode = ENCODERS
        .iter()
        .find(|(suffix, _)| key.ends_with(suffix))
        .map_or(plain as Encoder, |(_, encode)| *encode);
    encode(text).map(Bytes::from)
}

/// A fresh directory holding `files`, the source folder over it, and the directory to remove afterwards.
pub(crate) async fn folder_with(
    files: &[(&str, &str)],
) -> Result<(OpenFolder, std::path::PathBuf), String> {
    let root = std::env::temp_dir().join(format!("sqlload-unit-{}", uuid::Uuid::new_v4()));
    let folder = open_folder(&Location {
        store: StoreKind::Filesystem {
            root: root.display().to_string(),
        },
        prefix: "events/".to_owned(),
    })
    .map_err(|error| error.to_string())?;
    for (key, text) in files {
        write_file(&folder, key, file_bytes(key, text)?)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok((folder, root))
}

/// The table's columns as `name:type`, `name:!` for one without a type.
pub(crate) fn column_list(table: &FilesTable) -> String {
    let columns: Vec<String> = table
        .columns
        .iter()
        .map(|column| {
            let column_type = column
                .column_type
                .as_ref()
                .map_or("!".to_owned(), ToString::to_string);
            format!("{}:{column_type}", column.name)
        })
        .collect();
    columns.join(",")
}

/// Discover `files` and read every one with the found columns (`pinned` types win); the rows as `|`-separated values (`NULL` for NULL), one per line.
pub(crate) async fn read_rows(
    files: &[(&str, &str)],
    encoding: &FilesEncoding,
    pinned: &[(&str, ColumnType)],
) -> Result<String, String> {
    let (folder, root) = folder_with(files).await?;
    let result = discover_and_read(&folder, encoding, pinned).await;
    let _ = std::fs::remove_dir_all(&root);
    result
}

async fn discover_and_read(
    folder: &OpenFolder,
    encoding: &FilesEncoding,
    pinned: &[(&str, ColumnType)],
) -> Result<String, String> {
    let table = discover_files(folder, &[], encoding)
        .await
        .map_err(|error| error.to_string())?;
    let columns: Vec<DesiredColumn> = table
        .columns
        .iter()
        .map(|column| DesiredColumn {
            name: column.name.clone(),
            column_type: pinned
                .iter()
                .find(|(name, _)| *name == column.name)
                .map(|(_, column_type)| column_type.clone())
                .unwrap_or_else(|| column.column_type.clone().unwrap_or(ColumnType::Text)),
            origin: ColumnOrigin::Source,
        })
        .collect();
    let request = FilesRead {
        files: table.files,
        encoding: encoding.clone(),
        columns,
        batch_rows: 2,
    };
    let mut lines: Vec<String> = Vec::new();
    read_files(folder, &request, async |batch: RecordBatch| {
        lines.extend(row_texts(&batch)?);
        Ok(())
    })
    .await
    .map_err(|error| error.to_string())?;
    Ok(lines.join("\n"))
}

fn row_texts(batch: &RecordBatch) -> Result<Vec<String>, String> {
    let options = FormatOptions::default().with_null("NULL");
    let formatters: Vec<ArrayFormatter<'_>> = batch
        .columns()
        .iter()
        .map(|column| ArrayFormatter::try_new(column.as_ref(), &options))
        .collect::<Result<_, _>>()
        .map_err(|error| error.to_string())?;
    Ok((0..batch.num_rows())
        .map(|row| {
            let values: Vec<String> = formatters
                .iter()
                .map(|formatter| formatter.value(row).to_string())
                .collect();
            values.join("|")
        })
        .collect())
}
