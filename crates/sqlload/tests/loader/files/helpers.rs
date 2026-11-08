use arrow_array::RecordBatch;
use sqlload::file_store::main::list_files::list_files;
use sqlload::file_store::main::open_folder::open_folder;
use sqlload::file_store::main::read_file::read_file;
use sqlload::file_store::main::write_file::write_file;
use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::models::PlanOptions;
use sqlload::store_paths::models::Location;

use crate::helpers::{column_values, day_files, described_project, run_once};
use crate::test_types::FilesToFilesTestCase;

/// A run adding no source files.
pub(crate) fn no_files() -> Result<Vec<(String, bytes::Bytes)>, String> {
    Ok(Vec::new())
}

/// A run adding a third day's source file.
pub(crate) fn third_day() -> Result<Vec<(String, bytes::Bytes)>, String> {
    day_files("2026-11-03", (1, 0), None)
}

/// A run adding a late file to the first day and a new file to the second, the newest.
pub(crate) fn late_and_new() -> Result<Vec<(String, bytes::Bytes)>, String> {
    let rename =
        |(key, body): (String, bytes::Bytes), name: &str| (key.replace("part-00000", name), body);
    let mut files: Vec<(String, bytes::Bytes)> = day_files("2026-11-01", (1, 0), None)?
        .into_iter()
        .map(|file| rename(file, "part-late"))
        .collect();
    files.extend(
        day_files("2026-11-02", (1, 0), None)?
            .into_iter()
            .map(|file| rename(file, "part-new")),
    );
    Ok(files)
}

/// Load the case's files source into a files target once per run, adding that run's source files first; after each run, the rows the target holds and how many loads wrote them, as `rows|loads`.
pub(crate) async fn run_files_to_files(
    endpoint: &str,
    directory: &std::path::Path,
    test_case: &FilesToFilesTestCase,
) -> Result<Vec<String>, String> {
    let (store, connection) = (test_case.store)(endpoint, directory);
    let prefix = format!("f2f-{}/", uuid::Uuid::new_v4());
    let source = open_folder(&Location {
        store: store.clone(),
        prefix: format!("{prefix}src/"),
    })
    .map_err(|error| error.to_string())?;
    let output = open_folder(&Location {
        store,
        prefix: format!("{prefix}out/"),
    })
    .map_err(|error| error.to_string())?;
    let text = format!(
        "[connections.files]\n{connection}\n\n[targets.lake]\nconnection = \"files\"\npath = \"{prefix}out/{{table}}/\"\n\n[sources.orders]\nkind = \"files\"\ndescription = \"Test table.\"\nconnection = \"files\"\npath = \"{prefix}src/\"\npartition_columns = {{ day = \"date\" }}\n{}\n",
        test_case
            .settings
            .replace("PREFIX", prefix.trim_end_matches('/'))
    );
    let project = described_project(&text).await?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let mut held: Vec<String> = Vec::new();
    for files in test_case.runs {
        for (key, body) in files()? {
            write_file(&source, &key, body)
                .await
                .map_err(|error| error.to_string())?;
        }
        run_once(&text, &destinations, &PlanOptions::default())
            .await
            .map_err(|error| format!("{}: {error}", test_case.description))?;
        let mut batches: Vec<RecordBatch> = Vec::new();
        for file in list_files(&output)
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .filter(|file| file.relative_key.ends_with(".parquet"))
        {
            let bytes = read_file(&output, &file.relative_key)
                .await
                .map_err(|error| error.to_string())?;
            let reader =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
                    .and_then(|builder| builder.build())
                    .map_err(|error| error.to_string())?;
            for batch in reader {
                batches.push(batch.map_err(|error| error.to_string())?);
            }
        }
        let loads: std::collections::BTreeSet<String> =
            column_values(&batches, "_sqlload_load_id")?
                .into_iter()
                .collect();
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        held.push(format!("{rows}|{}", loads.len()));
    }
    Ok(held)
}
