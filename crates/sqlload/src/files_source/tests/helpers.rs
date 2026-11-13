use std::io::Write;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use flate2::Compression as GzipLevel;
use flate2::write::GzEncoder;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::arrow::{ARROW_SCHEMA_META_KEY, ArrowWriter, encode_arrow_schema};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::TypePtr;

use crate::column_types::models::ColumnType;
use crate::file_store::main::list_files::list_files;
use crate::file_store::main::open_folder::open_folder;
use crate::file_store::main::write_file::write_file;
use crate::file_store::models::{OpenFolder, StoredFile};
use crate::files_source::_helpers::text::csv_records::{FirstRecord, QuoteTracker};
use crate::files_source::_helpers::text::json_records::{JsonSplitter, flattened_record};
use crate::files_source::constants::RECORD_COLUMN;
use crate::files_source::main::after_load::after_load;
use crate::files_source::main::discover_files::discover_files;
use crate::files_source::main::move_to_prefix::move_to_prefix;
use crate::files_source::main::read_files::read_files;
use crate::files_source::models::{
    BatchFooters, Compression, CsvOptions, FilesEncoding, FilesRead, FilesTable, PathValues,
    SourceFormat, TextChecks, XlsxOptions, XmlOptions,
};
use crate::files_source::tests::test_types::{
    AfterLoadTestCase, FlattenJsonTestCase, ReadTextFilesTestCase, SplitJsonTestCase,
    TrackCsvTestCase,
};
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};
use crate::store_paths::main::resolve_location::resolve_location;
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
        record: None,
        ..FilesEncoding::default()
    }
}

/// CSV files with `csv` options.
pub(crate) fn csv_encoding(csv: CsvOptions) -> FilesEncoding {
    FilesEncoding {
        format: SourceFormat::Csv,
        compression: Compression::Auto,
        csv,
        record: None,
        ..FilesEncoding::default()
    }
}

/// `format` files read whole, one record each, into column `record`; CSV with `csv` options.
pub(crate) fn record_encoding(format: SourceFormat, csv: CsvOptions) -> FilesEncoding {
    FilesEncoding {
        format,
        compression: Compression::Auto,
        csv,
        record: Some(RECORD_COLUMN.to_owned()),
        ..FilesEncoding::default()
    }
}

/// Turns a test file's text into its bytes.
type Encoder = fn(&str) -> Result<Vec<u8>, String>;

/// How a test file is written, by a part of its key: the first that matches wins.
const ENCODERS: &[(&str, Encoder)] = &[
    (".zip", zip_archive),
    (".xlsx", workbook),
    (".gz", gzip),
    (".raw.", raw),
    (".utf16.", utf16),
    ("", plain),
];

fn plain(text: &str) -> Result<Vec<u8>, String> {
    Ok(text.as_bytes().to_vec())
}

/// Each character as the one byte of the same number, so `\u{e9}` writes byte 0xE9.
fn raw(text: &str) -> Result<Vec<u8>, String> {
    text.chars()
        .map(|character| {
            u8::try_from(u32::from(character)).map_err(|_| format!("{character:?} is not one byte"))
        })
        .collect()
}

/// UTF-16LE with its byte order mark.
fn utf16(text: &str) -> Result<Vec<u8>, String> {
    Ok([0xFF, 0xFE]
        .into_iter()
        .chain(text.encode_utf16().flat_map(u16::to_le_bytes))
        .collect())
}

/// A ZIP archive of the members `text` lists, separated by `\u{1e}`, each `name\u{1f}content`, with each member written as its own name says (so a member may be a workbook); a name ending in `/` is a folder.
fn zip_archive(text: &str) -> Result<Vec<u8>, String> {
    let mut archive = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    let options = zip::write::SimpleFileOptions::default();
    let members: Vec<(&str, &str)> = text
        .split('\u{1e}')
        .map(|member| member.split_once('\u{1f}').unwrap_or((member, "")))
        .collect();
    for (name, _) in members.iter().filter(|(name, _)| name.ends_with('/')) {
        archive
            .add_directory(*name, options)
            .map_err(|error| error.to_string())?;
    }
    for (name, content) in members.iter().filter(|(name, _)| !name.ends_with('/')) {
        archive
            .start_file(*name, options)
            .map_err(|error| error.to_string())?;
        archive
            .write_all(&file_bytes(name, content)?)
            .map_err(|error| error.to_string())?;
    }
    archive
        .finish()
        .map(std::io::Cursor::into_inner)
        .map_err(|error| error.to_string())
}

/// Writes one workbook cell's text, its type prefix removed.
type CellWriter =
    fn(&mut rust_xlsxwriter::Worksheet, (u32, u16), &str) -> Result<(), rust_xlsxwriter::XlsxError>;

/// How a workbook cell is written, by its prefix: the first that matches wins, and a cell without one is a string.
const CELL_WRITERS: &[(&str, CellWriter)] = &[
    ("n:", |sheet, (row, column), text| {
        sheet
            .write_number(row, column, text.parse::<f64>().unwrap_or(f64::NAN))
            .map(|_| ())
    }),
    ("b:", |sheet, (row, column), text| {
        sheet.write_boolean(row, column, text == "true").map(|_| ())
    }),
    ("dt:", |sheet, place, text| {
        moment(sheet, place, (text, "yyyy-mm-dd hh:mm:ss"))
    }),
    ("d:", |sheet, place, text| {
        moment(sheet, place, (text, "yyyy-mm-dd"))
    }),
    ("tm:", |sheet, place, text| {
        moment(sheet, place, (text, "hh:mm:ss"))
    }),
    ("s:", |sheet, (row, column), text| {
        sheet.write_string(row, column, text).map(|_| ())
    }),
    ("", |sheet, (row, column), text| {
        sheet.write_string(row, column, text).map(|_| ())
    }),
];

/// An xlsx workbook of the sheets in `text`: each starts with a `[Name]` line, then one line per row (from row 1) with cells separated by `|`. A cell is a string, empty, or typed by a prefix: `n:` number, `b:` boolean, `d:` date, `dt:` date and time, `tm:` time of day, `s:` string as written.
fn workbook(text: &str) -> Result<Vec<u8>, String> {
    let failed = |error: rust_xlsxwriter::XlsxError| error.to_string();
    let mut book = rust_xlsxwriter::Workbook::new();
    for sheet in text.split('[').filter(|sheet| !sheet.is_empty()) {
        let (name, rows) = sheet.split_once("]\n").unwrap_or((sheet, ""));
        let worksheet = book.add_worksheet();
        worksheet.set_name(name).map_err(failed)?;
        for (row, line) in rows.lines().enumerate() {
            let cells = line
                .split('|')
                .enumerate()
                .filter(|(_, cell)| !cell.is_empty());
            for (column, cell) in cells {
                let place = (
                    u32::try_from(row).map_err(|error| error.to_string())?,
                    u16::try_from(column).map_err(|error| error.to_string())?,
                );
                let (prefix, write) = CELL_WRITERS
                    .iter()
                    .find(|(prefix, _)| cell.starts_with(prefix))
                    .ok_or_else(|| format!("no writer for cell {cell:?}"))?;
                write(worksheet, place, &cell[prefix.len()..]).map_err(failed)?;
            }
        }
    }
    book.save_to_buffer().map_err(failed)
}

/// A date, time or both, written with number `format`.
fn moment(
    sheet: &mut rust_xlsxwriter::Worksheet,
    (row, column): (u32, u16),
    (text, format): (&str, &str),
) -> Result<(), rust_xlsxwriter::XlsxError> {
    let value = rust_xlsxwriter::ExcelDateTime::parse_from_str(text)?;
    let format = rust_xlsxwriter::Format::new().set_num_format(format);
    sheet
        .write_datetime_with_format(row, column, value, &format)
        .map(|_| ())
}

fn gzip(text: &str) -> Result<Vec<u8>, String> {
    let mut encoder = GzEncoder::new(Vec::new(), GzipLevel::default());
    encoder
        .write_all(text.as_bytes())
        .map_err(|error| error.to_string())?;
    encoder.finish().map_err(|error| error.to_string())
}

/// `text` gzip-compressed when the key has `.gz`, byte for byte with `.raw.`, as UTF-16LE with `.utf16.`, as written otherwise.
fn file_bytes(key: &str, text: &str) -> Result<Bytes, String> {
    let encode = ENCODERS
        .iter()
        .find(|(part, _)| key.contains(part))
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
pub(crate) async fn read_rows(test_case: &ReadTextFilesTestCase<'_>) -> Result<String, String> {
    let (folder, root) = folder_with(test_case.files).await?;
    let result = discover_and_read(&folder, test_case).await;
    let _ = std::fs::remove_dir_all(&root);
    result
}

async fn discover_and_read(
    folder: &OpenFolder,
    test_case: &ReadTextFilesTestCase<'_>,
) -> Result<String, String> {
    let table = discover_files(folder, &PathValues::default(), &test_case.encoding)
        .await
        .map_err(|error| error.to_string())?;
    write_files(folder, test_case.unsampled).await?;
    let listed = discover_files(folder, &PathValues::default(), &test_case.encoding)
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
        nanos: Vec::new(),
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

/// The case's object flattened as `naming = \"dlt\"` flattens it, shown as the case's `expected` describes.
pub(crate) fn flatten_json(test_case: &FlattenJsonTestCase) -> Result<String, String> {
    let mut splitter = JsonSplitter::new(true, 1024);
    let mut records = splitter.push(test_case.text.as_bytes())?;
    records.extend(splitter.finish()?);
    let (record, made) = flattened_record(records.remove(0))?;
    let fields: Vec<String> = record
        .fields
        .iter()
        .map(|(name, value, nested)| {
            format!(
                "{name}={}",
                nested.clone().unwrap_or_else(|| value.to_string())
            )
        })
        .collect();
    let made: Vec<String> = made.into_iter().collect();
    Ok(format!("{} made {}", fields.join("|"), made.join(",")))
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
    let listed = discover_files(folder, &PathValues::default(), &encoding)
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
        nanos: Vec::new(),
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

/// One Parquet file of three rows with nested columns, discovered and read: column `name`'s planned type, and its values `|`-separated (`NULL` for NULL).
pub(crate) async fn nested_parquet_column(name: &str) -> Result<(String, String), String> {
    let (folder, root) = folder_with(&[]).await?;
    let result = write_and_read_nested(&folder, name).await;
    let _ = std::fs::remove_dir_all(&root);
    result
}

/// The nested test file's columns: `tags` (list of text), `point` (struct), `attrs` (map of text to int), `grid` (list of lists), `opt` (a struct with a field of Arrow's NULL type, as pyarrow writes one always None) and `codes` (a list of dictionary-encoded ints).
fn nested_batch() -> Result<RecordBatch, String> {
    use arrow_array::builder::{Int64Builder, ListBuilder, MapBuilder, StringBuilder};
    use arrow_array::{ArrayRef, Float64Array, Int32Array, StructArray};
    let mut tags = ListBuilder::new(StringBuilder::new());
    tags.append_value([Some("a"), None, Some("say \"hi\"")]);
    tags.append_value(Vec::<Option<&str>>::new());
    tags.append_null();
    let point = StructArray::from(vec![
        (
            Arc::new(Field::new("x", DataType::Float64, true)),
            Arc::new(Float64Array::from(vec![Some(1.5), Some(-2.25), None])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("y", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
        ),
    ]);
    let mut attrs = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    attrs.keys().append_value("k");
    attrs.values().append_value(1);
    attrs.append(true).map_err(|error| error.to_string())?;
    attrs.append(true).map_err(|error| error.to_string())?;
    attrs.keys().append_value("a");
    attrs.values().append_null();
    attrs.append(true).map_err(|error| error.to_string())?;
    let mut grid = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
    grid.values().append_value([Some(1), Some(2)]);
    grid.values().append_value([Some(3)]);
    grid.append(true);
    grid.append(true);
    grid.values().append_null();
    grid.append(true);
    let opt = StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::Null, true)),
            arrow_array::new_null_array(&DataType::Null, 3),
        ),
        (
            Arc::new(Field::new("b", DataType::Int64, true)),
            Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        ),
    ]);
    let mut plain_codes = ListBuilder::new(Int64Builder::new());
    plain_codes.append_value([Some(5), Some(7)]);
    plain_codes.append_value([Some(5)]);
    plain_codes.append_value(Vec::<Option<i64>>::new());
    let codes = arrow_cast::cast(
        &plain_codes.finish(),
        &DataType::List(Arc::new(Field::new(
            "item",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int64)),
            true,
        ))),
    )
    .map_err(|error| error.to_string())?;
    RecordBatch::try_from_iter([
        ("opt", Arc::new(opt) as ArrayRef),
        ("codes", codes),
        ("tags", Arc::new(tags.finish()) as ArrayRef),
        ("point", Arc::new(point) as ArrayRef),
        ("attrs", Arc::new(attrs.finish()) as ArrayRef),
        ("grid", Arc::new(grid.finish()) as ArrayRef),
    ])
    .map_err(|error| error.to_string())
}

async fn write_and_read_nested(
    folder: &OpenFolder,
    name: &str,
) -> Result<(String, String), String> {
    let batch = nested_batch()?;
    let mut body: Vec<u8> = Vec::new();
    let mut writer = parquet::arrow::ArrowWriter::try_new(&mut body, batch.schema(), None)
        .map_err(|error| error.to_string())?;
    writer.write(&batch).map_err(|error| error.to_string())?;
    writer.close().map_err(|error| error.to_string())?;
    write_file(folder, "part.parquet", Bytes::from(body))
        .await
        .map_err(|error| error.to_string())?;
    let encoding = FilesEncoding::default();
    let listed = discover_files(folder, &PathValues::default(), &encoding)
        .await
        .map_err(|error| error.to_string())?;
    let column_type = listed
        .columns
        .iter()
        .find(|column| column.name == name)
        .map(|column| column.column_type.clone())
        .ok_or_else(|| format!("no column {name}"))??;
    let request = FilesRead {
        nanos: Vec::new(),
        files: listed.files,
        encoding,
        checks: TextChecks::default(),
        columns: vec![DesiredColumn {
            name: name.to_owned(),
            column_type: column_type.clone(),
            origin: ColumnOrigin::Source,
        }],
        batch_rows: 10,
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
    Ok((column_type.to_string(), lines.join("|")))
}

/// The type the reader finds in the footer of a file written with a column of `written`, with or without the embedded Arrow schema; other writers' files often have none.
pub(crate) fn footer_type(written: &DataType, embedded_schema: bool) -> Result<DataType, String> {
    let column = arrow_array::new_null_array(written, 1);
    let batch = RecordBatch::try_from_iter([("at", column)]).map_err(|error| error.to_string())?;
    let options = ArrowWriterOptions::new().with_skip_arrow_metadata(!embedded_schema);
    let mut buffer: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new_with_options(&mut buffer, batch.schema(), options)
        .map_err(|error| error.to_string())?;
    writer.write(&batch).map_err(|error| error.to_string())?;
    writer.close().map_err(|error| error.to_string())?;
    let footer = ArrowReaderMetadata::load(&Bytes::from(buffer), ArrowReaderOptions::new())
        .map_err(|error| error.to_string())?;
    Ok(footer.schema().field(0).data_type().clone())
}

/// The footer of a file with this Parquet schema and, over it, an embedded Arrow schema giving column `at` this type: what another writer can produce, and `ArrowWriter` cannot.
pub(crate) fn overridden_footer(
    message: &str,
    embedded: Option<&DataType>,
) -> Result<ArrowReaderMetadata, String> {
    let schema = parse_message_type(message).map_err(|error| error.to_string())?;
    let metadata = embedded.map(|data_type| {
        let arrow = Schema::new(vec![Field::new("at", data_type.clone(), true)]);
        vec![KeyValue::new(
            ARROW_SCHEMA_META_KEY.to_owned(),
            encode_arrow_schema(&arrow),
        )]
    });
    let properties = WriterProperties::builder()
        .set_key_value_metadata(metadata)
        .build();
    let mut buffer: Vec<u8> = Vec::new();
    SerializedFileWriter::new(&mut buffer, Arc::new(schema), Arc::new(properties))
        .and_then(|writer| writer.close())
        .map_err(|error| error.to_string())?;
    ArrowReaderMetadata::load(&Bytes::from(buffer), ArrowReaderOptions::new())
        .map_err(|error| error.to_string())
}

/// The only column of `message`, or `None` with no `message`.
pub(crate) fn only_column(message: Option<&str>) -> Result<Option<TypePtr>, String> {
    let parsed = message.map(parse_message_type).transpose();
    let schema = parsed.map_err(|error| error.to_string())?;
    Ok(schema.and_then(|schema| schema.get_fields().first().cloned()))
}

/// A timestamp type of `unit` in `zone`.
pub(crate) fn timestamp(
    unit: arrow_schema::TimeUnit,
    zone: Option<&str>,
) -> arrow_schema::DataType {
    arrow_schema::DataType::Timestamp(unit, zone.map(std::sync::Arc::from))
}

/// The portable type `footer_columns` reads for the one column of a Parquet file written with a column of `written`.
pub(crate) fn footer_column_type(written: &DataType) -> Result<String, String> {
    let column = arrow_array::new_null_array(written, 1);
    let batch =
        RecordBatch::try_from_iter([("values", column)]).map_err(|error| error.to_string())?;
    let mut buffer: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None)
        .map_err(|error| error.to_string())?;
    writer.write(&batch).map_err(|error| error.to_string())?;
    writer.close().map_err(|error| error.to_string())?;
    let read = ArrowReaderMetadata::load(&Bytes::from(buffer), ArrowReaderOptions::new())
        .map_err(|error| error.to_string())?;
    let footer = crate::files_source::models::Footer {
        relative_key: "part.parquet".to_owned(),
        schema: read.schema().clone(),
        metadata: read.metadata().clone(),
    };
    let (columns, _) =
        crate::files_source::main::footer_columns::footer_columns(std::slice::from_ref(&footer));
    let column = columns.first().ok_or("no column read")?;
    column
        .column_type
        .as_ref()
        .map(ToString::to_string)
        .map_err(Clone::clone)
}

/// CSV options reading `encoding`, skipping `skip_rows` lines.
pub(crate) fn decoded(encoding: &str, skip_rows: u64) -> CsvOptions {
    CsvOptions {
        encoding: encoding.to_owned(),
        skip_rows,
        ..CsvOptions::default()
    }
}

/// `format` files, with `xlsx` and `xml` options, in ZIP archives when `members` is set (`Some("")` for any member).
pub(crate) fn container_encoding(
    format: SourceFormat,
    (xlsx, xml): (XlsxOptions, XmlOptions),
    members: Option<&str>,
) -> FilesEncoding {
    FilesEncoding {
        format,
        compression: members.map_or(Compression::Auto, |_| Compression::Zip),
        xlsx,
        xml,
        members: members
            .filter(|pattern| !pattern.is_empty())
            .map(str::to_owned),
        ..FilesEncoding::default()
    }
}

/// XML options with rows at `record_path` and `lists` loaded as arrays.
pub(crate) fn xml_at(record_path: &str, lists: &[&str]) -> XmlOptions {
    XmlOptions {
        record_path: record_path.to_owned(),
        lists: lists.iter().map(|name| (*name).to_owned()).collect(),
    }
}

/// Fresh directories to run a local store case in, each with the root a store opens it at: the path itself, and on Windows also the same folder through the `C$` share on `localhost`, as a UNC path.
pub(crate) fn local_roots() -> Vec<(std::path::PathBuf, String)> {
    let fresh = || std::env::temp_dir().join(format!("sqlload-unit-{}", uuid::Uuid::new_v4()));
    let dir = fresh();
    let roots = vec![(dir.clone(), dir.display().to_string())];
    #[cfg(windows)]
    let roots = {
        let mut roots = roots;
        let dir = fresh();
        let path = dir.display().to_string().replace('\\', "/");
        let (drive, rest) = path.split_at(1);
        let share = format!("//localhost/{drive}$/{}", rest.trim_start_matches(":/"));
        roots.push((dir, share));
        roots
    };
    roots
}

/// Write `test_case`'s files below `dir`, list its loaded files under `events/`, rewrite its `rewritten` files, run its `after_load` on the listed ones, opened at `root`, and return the report as `action status files failed` (`none` without one) and every file left below `dir` as `path=contents`, sorted.
pub(crate) async fn run_after_load(
    (dir, root): &(std::path::PathBuf, String),
    test_case: &AfterLoadTestCase,
) -> Result<(String, Vec<String>), String> {
    for (key, text) in test_case.files {
        let path = dir.join(key);
        std::fs::create_dir_all(path.parent().unwrap_or(dir)).map_err(|error| error.to_string())?;
        std::fs::write(&path, text).map_err(|error| error.to_string())?;
    }
    let folder = open_folder(&Location {
        store: StoreKind::Filesystem { root: root.clone() },
        prefix: "events/".to_owned(),
    })
    .map_err(|error| error.to_string())?;
    let loaded: Vec<StoredFile> = list_files(&folder)
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .filter(|file| test_case.loaded.contains(&file.relative_key.as_str()))
        .collect();
    for (key, text) in test_case.rewritten {
        std::fs::write(dir.join(key), text).map_err(|error| error.to_string())?;
    }
    let report = after_load(&folder, &test_case.after_load, &loaded)
        .await
        .map_or("none".to_owned(), |report| {
            format!(
                "{} {} {} {}",
                report.action,
                report.status,
                report.files,
                report.failed.len()
            )
        });
    let everything = open_folder(&Location {
        store: StoreKind::Filesystem {
            root: dir.display().to_string(),
        },
        prefix: String::new(),
    })
    .map_err(|error| error.to_string())?;
    let mut files = Vec::new();
    for file in list_files(&everything)
        .await
        .map_err(|error| error.to_string())?
    {
        let text = std::fs::read_to_string(dir.join(&file.relative_key))
            .map_err(|error| error.to_string())?;
        files.push(format!("{}={text}", file.relative_key));
    }
    files.sort();
    std::fs::remove_dir_all(dir).map_err(|error| error.to_string())?;
    Ok((report, files))
}

/// `move_to_prefix` for `move_to` and the source's `path`, both under the connection `store`.
pub(crate) fn resolved_move_to(
    store: &StoreKind,
    path: &str,
    move_to: &str,
) -> Result<String, String> {
    let location =
        resolve_location(Some(("lake", store)), path).map_err(|error| error.to_string())?;
    move_to_prefix(&location, move_to)
}

/// A filesystem connection's store at `root`.
pub(crate) fn local(root: &str) -> StoreKind {
    StoreKind::Filesystem {
        root: root.to_owned(),
    }
}
