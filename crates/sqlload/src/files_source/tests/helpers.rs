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
use crate::files_source::_helpers::text::csv_records::{FirstRecord, QuoteTracker};
use crate::files_source::_helpers::text::json_records::JsonSplitter;
use crate::files_source::main::discover_files::discover_files;
use crate::files_source::main::read_files::read_files;
use crate::files_source::models::{
    BatchFooters, Compression, CsvOptions, FilesEncoding, FilesRead, FilesTable, SourceFormat,
    TextChecks,
};
use crate::files_source::tests::test_types::{
    ReadTextFilesTestCase, SplitJsonTestCase, TrackCsvTestCase,
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
    write_files(&folder, files).await?;
    Ok((folder, root))
}

async fn write_files(folder: &OpenFolder, files: &[(&str, &str)]) -> Result<(), String> {
    for (key, text) in files {
        write_file(folder, key, file_bytes(key, text)?)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
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

/// Discover the test case's files, add its unsampled files, and read them all with the found columns (pinned types win); the rows as `|`-separated values (`NULL` for NULL), one per line.
pub(crate) async fn read_rows(test_case: &ReadTextFilesTestCase) -> Result<String, String> {
    let (folder, root) = folder_with(test_case.files).await?;
    let result = discover_and_read(&folder, test_case).await;
    let _ = std::fs::remove_dir_all(&root);
    result
}

async fn discover_and_read(
    folder: &OpenFolder,
    test_case: &ReadTextFilesTestCase,
) -> Result<String, String> {
    let table = discover_files(folder, &[], &test_case.encoding)
        .await
        .map_err(|error| error.to_string())?;
    write_files(folder, test_case.unsampled).await?;
    let listed = discover_files(folder, &[], &test_case.encoding)
        .await
        .map_err(|error| error.to_string())?;
    let pinned = test_case.pinned;
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
    let names: Vec<String> = table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let checks = TextChecks {
        known: names.clone(),
        exclude: test_case
            .exclude
            .iter()
            .map(|pattern| (*pattern).to_owned())
            .collect(),
        allow: Vec::new(),
        inferred: names
            .into_iter()
            .filter(|name| !pinned.iter().any(|(pinned, _)| pinned == name))
            .collect(),
    };
    let request = FilesRead {
        files: listed.files,
        encoding: test_case.encoding.clone(),
        checks,
        columns,
        batch_rows: 2,
    };
    let mut lines: Vec<String> = Vec::new();
    read_files(
        folder,
        &request,
        &BatchFooters::default(),
        async |batch: RecordBatch| {
            lines.extend(row_texts(&batch)?);
            Ok(())
        },
    )
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

/// The lines of the objects in the test case's file, fed in chunks.
pub(crate) fn split_json(test_case: &SplitJsonTestCase) -> Result<Vec<u64>, String> {
    let mut splitter = JsonSplitter::new(test_case.by_line, test_case.max_record_bytes);
    let mut lines: Vec<u64> = Vec::new();
    for chunk in test_case.text.as_bytes().chunks(test_case.chunk_bytes) {
        lines.extend(splitter.push(chunk)?.into_iter().map(|record| record.line));
    }
    lines.extend(splitter.finish()?.into_iter().map(|record| record.line));
    Ok(lines)
}

/// The first record's fields joined by `|`, once the whole file was followed for open quotes.
pub(crate) fn read_csv(test_case: &TrackCsvTestCase) -> Result<String, String> {
    let mut first_record = FirstRecord::new(b',', b'"', test_case.max_record_bytes);
    let mut tracker = QuoteTracker::new(b',', b'"', test_case.max_record_bytes);
    let mut first: Option<Vec<String>> = None;
    for chunk in test_case
        .text
        .as_bytes()
        .chunks(test_case.chunk_bytes)
        .chain([&b""[..]])
    {
        first = first.map_or_else(|| first_record.feed(chunk), |fields| Ok(Some(fields)))?;
        tracker.push(chunk)?;
    }
    tracker.finish()?;
    Ok(first.unwrap_or_default().join("|"))
}

/// A Parquet file whose footer breaks after `batch_footers` read it (when `reuse`), read back: only reading the footer again can fail. The rows read, or `failed`.
pub(crate) async fn read_with_broken_footer(reuse: bool) -> Result<String, String> {
    let (folder, root) = folder_with(&[]).await?;
    let result = break_footer_and_read(&folder, reuse).await;
    let _ = std::fs::remove_dir_all(&root);
    result
}

async fn break_footer_and_read(folder: &OpenFolder, reuse: bool) -> Result<String, String> {
    let values: arrow_array::ArrayRef =
        std::sync::Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3]));
    let batch = RecordBatch::try_from_iter([("id", values)]).map_err(|error| error.to_string())?;
    let mut body: Vec<u8> = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut body, batch.schema(), None)
        .map_err(|error| error.to_string())?;
    writer.write(&batch).map_err(|error| error.to_string())?;
    writer.close().map_err(|error| error.to_string())?;
    let body_bytes = Bytes::from(body.clone());
    write_file(folder, "part.parquet", body_bytes)
        .await
        .map_err(|error| error.to_string())?;
    let encoding = FilesEncoding::default();
    let listed = discover_files(folder, &[], &encoding)
        .await
        .map_err(|error| error.to_string())?;
    let footers = crate::files_source::main::batch_footers::batch_footers(folder, &listed.files)
        .await
        .map_err(|error| error.to_string())?;
    let kept = [BatchFooters::default(), footers][usize::from(reuse)].clone();
    let broken_from = body.len() - 4;
    body[broken_from..].fill(0);
    write_file(folder, "part.parquet", Bytes::from(body))
        .await
        .map_err(|error| error.to_string())?;
    let request = FilesRead {
        files: listed.files,
        encoding,
        checks: TextChecks::default(),
        columns: vec![DesiredColumn {
            name: "id".to_owned(),
            column_type: ColumnType::Int,
            origin: ColumnOrigin::Source,
        }],
        batch_rows: 10,
    };
    Ok(read_files(folder, &request, &kept, async |_| Ok(()))
        .await
        .map_or_else(|_| "failed".to_owned(), |rows| rows.to_string()))
}
