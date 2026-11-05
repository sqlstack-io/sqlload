//! Instruction counts of sqlload's hot paths under Callgrind (gungraun); inputs are built in setup functions, which are not measured.
//!
//! Run with `make bench-perf`; CI compares a pull request with its merge base and fails on a
//! regression above 5% in any benchmark's instruction count.

mod fixtures;

use std::hint::black_box;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use gungraun::{library_benchmark, library_benchmark_group, main};
use sqlload::column_types::models::ColumnType;
use sqlload::file_store::models::OpenFolder;
use sqlload::files_destination::main::close_writer::close_writer;
use sqlload::files_destination::main::open_writer::open_writer;
use sqlload::files_destination::main::write_rows::write_rows;
use sqlload::files_destination::models::{TablePath, WriterRequest};
use sqlload::files_source::main::discover_files::discover_files;
use sqlload::files_source::main::read_files::read_files;
use sqlload::files_source::models::{FilesEncoding, FilesRead, SourceFormat, TextChecks};
use sqlload::postgres_source::main::decode_copy::decode_copy;
use sqlload::row_batches::main::columns_batch::columns_batch;
use sqlload::row_batches::main::conform_batch::conform_batch;
use sqlload::row_batches::main::json_rows::json_rows;
use sqlload::row_batches::main::merge_schema::merge_schema;
use sqlload::row_batches::main::settled_schema::settled_schema;
use sqlload::row_batches::models::{JsonLayout, JsonRead};
use sqlload::schema_plan::models::{ColumnOrigin, DesiredColumn};
use tokio::runtime::Runtime;

use crate::fixtures::{CopyStream, Rows};

/// Rows per batch, as the loader reads sources.
const BATCH_ROWS: usize = 65_536;
/// Rows per batch the Python crossing gathers.
const CROSSING_ROWS: usize = 10_000;
const TEXT_FILES: u64 = 4;
const ROWS_PER_TEXT_FILE: u64 = 2_500;

fn narrow_copy() -> CopyStream {
    fixtures::narrow_stream(20_000)
}

fn wide_copy() -> CopyStream {
    fixtures::wide_stream(2_000)
}

#[library_benchmark]
#[bench::narrow_20k_rows(setup = narrow_copy)]
#[bench::wide_2k_rows(setup = wide_copy)]
fn postgres_copy_to_arrow(stream: CopyStream) -> Vec<RecordBatch> {
    let chunks = stream.chunks.iter().map(Vec::as_slice);
    black_box(
        decode_copy("public.bench", stream.columns, chunks, BATCH_ROWS)
            .unwrap_or_else(|error| panic!("{error}")),
    )
}

/// A runtime, an in-memory folder, the table's columns and its rows.
type ParquetInput = (Runtime, OpenFolder, Vec<DesiredColumn>, RecordBatch);

fn narrow_parquet_input() -> ParquetInput {
    let batch = fixtures::narrow_batch(50_000);
    let columns = desired_columns(&batch.schema(), ColumnOrigin::Source);
    let (runtime, folder) = fixtures::memory_folder(Vec::new());
    (runtime, folder, columns, batch)
}

fn desired_columns(schema: &SchemaRef, origin: ColumnOrigin) -> Vec<DesiredColumn> {
    let column_type = |field: &arrow_schema::Field| match field.data_type() {
        arrow_schema::DataType::Int64 | arrow_schema::DataType::Int32 => ColumnType::Int,
        arrow_schema::DataType::Utf8 => ColumnType::Text,
        arrow_schema::DataType::Boolean => ColumnType::Bool,
        arrow_schema::DataType::Float64 => ColumnType::Float,
        arrow_schema::DataType::Decimal128(precision, scale) => ColumnType::Decimal {
            precision: *precision,
            scale: *scale as u8,
        },
        arrow_schema::DataType::Timestamp(_, _) => ColumnType::TimestampTz,
        other => panic!("no benchmark column type for {other}"),
    };
    schema
        .fields()
        .iter()
        .map(|field| DesiredColumn {
            name: field.name().clone(),
            column_type: column_type(field),
            origin,
        })
        .collect()
}

#[library_benchmark]
#[bench::narrow_50k_rows(setup = narrow_parquet_input)]
fn arrow_to_parquet((runtime, folder, columns, batch): ParquetInput) -> Vec<String> {
    let path = TablePath {
        folder: folder.location.clone(),
        partition: None,
    };
    let request = WriterRequest {
        folder,
        path: &path,
        cursor_column: None,
        columns: &columns,
        load_id: "20261105T000000Z",
        loaded_at: "2026-11-05T00:00:00Z",
    };
    let mut writer = open_writer(request).unwrap_or_else(|error| panic!("{error}"));
    runtime.block_on(async {
        for offset in (0..batch.num_rows()).step_by(10_000) {
            let part = batch.slice(offset, 10_000.min(batch.num_rows() - offset));
            write_rows(&mut writer, part)
                .await
                .unwrap_or_else(|error| panic!("{error}"));
        }
        close_writer(&mut writer, true)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
    });
    black_box(writer.written)
}

/// A runtime, an in-memory folder of text files, and how they are encoded.
type TextInput = (Runtime, OpenFolder, FilesEncoding);

fn text_input(format: SourceFormat) -> TextInput {
    let (extension, render): (&str, fn(u64, u64) -> String) = match format {
        SourceFormat::Ndjson => ("ndjson", fixtures::narrow_ndjson),
        SourceFormat::Json => ("json", fixtures::narrow_json),
        _ => ("csv", fixtures::narrow_csv),
    };
    let files = (0..TEXT_FILES)
        .map(|part| {
            let first = 1 + part * ROWS_PER_TEXT_FILE;
            (
                format!("part-{part:03}.{extension}"),
                render(first, ROWS_PER_TEXT_FILE),
            )
        })
        .collect();
    let (runtime, folder) = fixtures::memory_folder(files);
    let encoding = FilesEncoding {
        format,
        ..FilesEncoding::default()
    };
    (runtime, folder, encoding)
}

fn ndjson_files() -> TextInput {
    text_input(SourceFormat::Ndjson)
}

fn json_files() -> TextInput {
    text_input(SourceFormat::Json)
}

fn csv_files() -> TextInput {
    text_input(SourceFormat::Csv)
}

/// Sample the files and infer their columns (as `plan` does), then read every file into typed batches (as `load` does).
#[library_benchmark]
#[bench::ndjson_4x2500_rows(setup = ndjson_files)]
#[bench::json_4x2500_rows(setup = json_files)]
#[bench::csv_4x2500_rows(setup = csv_files)]
fn text_files_infer_and_read((runtime, folder, encoding): TextInput) -> u64 {
    runtime.block_on(async {
        let table = discover_files(&folder, &[], &encoding)
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        let columns: Vec<DesiredColumn> = table
            .columns
            .iter()
            .filter_map(|column| {
                column.column_type.clone().ok().map(|column_type| DesiredColumn {
                    name: column.name.clone(),
                    column_type,
                    origin: ColumnOrigin::Inferred,
                })
            })
            .collect();
        let names: Vec<String> = columns.iter().map(|column| column.name.clone()).collect();
        let request = FilesRead {
            files: table.files,
            encoding,
            checks: TextChecks {
                known: names.clone(),
                inferred: names,
                ..TextChecks::default()
            },
            columns,
            batch_rows: BATCH_ROWS,
        };
        let rows = read_files(&folder, &request, async |batch| {
            black_box(batch);
            Ok(())
        })
        .await
        .unwrap_or_else(|error| panic!("{error}"));
        black_box(rows)
    })
}

fn flat_rows() -> Rows {
    fixtures::flat_rows(CROSSING_ROWS as u64)
}

/// Yielded dicts gathered column by column, then typed into one batch by inference.
#[library_benchmark]
#[bench::flat_10k_rows(setup = flat_rows)]
fn rows_to_batch(rows: Rows) -> RecordBatch {
    black_box(columns_batch(fixtures::gather(&rows)).unwrap_or_else(|error| panic!("{error}")))
}

fn drifting() -> Vec<RecordBatch> {
    fixtures::drifting_batches(12, 2_000)
}

/// What the loader does with a Python table's batches: merge their schemas, settle it, and conform every batch to it.
#[library_benchmark]
#[bench::drifting_12x2k_rows(setup = drifting)]
fn merge_and_conform(batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    let merged = batches
        .iter()
        .fold(arrow_schema::Schema::empty(), |seen, batch| {
            merge_schema(&seen, &batch.schema())
        });
    let schema = std::sync::Arc::new(settled_schema(&merged));
    black_box(
        batches
            .iter()
            .map(|batch| conform_batch(batch, &schema).unwrap_or_else(|error| panic!("{error}")))
            .collect(),
    )
}

/// JSON bytes and how to read them.
type JsonInput = (Vec<u8>, JsonRead);

fn lines(bytes: Vec<u8>) -> JsonInput {
    let read = JsonRead {
        layout: JsonLayout::Lines,
        path: None,
        batch_rows: CROSSING_ROWS,
    };
    (bytes, read)
}

fn flat_ndjson() -> JsonInput {
    lines(fixtures::flat_ndjson(20_000))
}

fn nested_ndjson() -> JsonInput {
    lines(fixtures::nested_ndjson(5_000))
}

fn flat_document() -> JsonInput {
    let read = JsonRead {
        layout: JsonLayout::Document,
        path: Some("data.items".to_owned()),
        batch_rows: CROSSING_ROWS,
    };
    (fixtures::flat_document(20_000), read)
}

/// The Rust side of `sl.json(...)` / `sl.ndjson(...)`: parse the bytes into rows and type each batch, as the crossing's parser threads do.
#[library_benchmark]
#[bench::ndjson_flat_20k_rows(setup = flat_ndjson)]
#[bench::ndjson_nested_5k_rows(setup = nested_ndjson)]
#[bench::document_path_flat_20k_rows(setup = flat_document)]
fn json_bytes_to_batches((bytes, read): JsonInput) -> Vec<RecordBatch> {
    let mut batches = Vec::new();
    json_rows(&bytes, &read, |columns| {
        batches.push(columns_batch(columns)?);
        Ok(())
    })
    .unwrap_or_else(|error| panic!("{error}"));
    black_box(batches)
}

library_benchmark_group!(name = postgres_source, benchmarks = postgres_copy_to_arrow);
library_benchmark_group!(name = files_destination, benchmarks = arrow_to_parquet);
library_benchmark_group!(name = files_source, benchmarks = text_files_infer_and_read);
library_benchmark_group!(name = row_batches, benchmarks = [rows_to_batch, merge_and_conform]);
library_benchmark_group!(name = python_crossing, benchmarks = json_bytes_to_batches);

main!(
    library_benchmark_groups = postgres_source,
    files_destination,
    files_source,
    row_batches,
    python_crossing
);
