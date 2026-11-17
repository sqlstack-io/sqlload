use arrow_array::RecordBatch;
use sqlload::adapters::dispatch::main::session::execute::execute;
use sqlload::adapters::dispatch::main::session::single_text::single_text;
use sqlload::adapters::dispatch::models::Destination;
use sqlload::file_store::main::list_files::list_files;
use sqlload::file_store::main::open_folder::open_folder;
use sqlload::file_store::main::read_file::read_file;
use sqlload::file_store::main::write_file::write_file;
use sqlload::loader::main::apply_project::apply_project;
use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::main::plan_project::plan_project;
use sqlload::loader::main::render_plan::render_plan;
use sqlload::loader::models::PlanOptions;
use sqlload::store_paths::models::Location;

use crate::helpers::{
    column_values, day_files, described_project, duckdb_text, load_with_options, local_store,
    parquet_bytes, run_once, run_sqlload, two_days, words,
};
use crate::test_types::{
    ColumnNamesTestCase, FilesToFilesTestCase, NamesOutcome, ParquetFile, PlantedTestCase,
    SourceRowUpgradeTestCase,
};

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

/// Load the case's files source into a files target once per run, adding that run's source files first; after each run, the rows the target holds and how many loads wrote them, as `rows|loads`, then every run's rendered plan.
pub(crate) async fn run_files_to_files(
    endpoint: &str,
    directory: &std::path::Path,
    test_case: &FilesToFilesTestCase,
) -> Result<(Vec<String>, String), String> {
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
    let (mut held, mut plan) = (Vec::new(), String::new());
    for files in test_case.runs {
        for (key, body) in files()? {
            write_file(&source, &key, body)
                .await
                .map_err(|error| error.to_string())?;
        }
        plan += &run_once(&text, &destinations, &PlanOptions::default())
            .await
            .map_err(|error| format!("{}: {error}", test_case.description))?;
        held.push(held_rows(&output).await?);
    }
    Ok((held, plan))
}

/// The rows the files in `output` hold and how many loads wrote them, as `rows|loads`.
async fn held_rows(output: &sqlload::file_store::models::OpenFolder) -> Result<String, String> {
    let mut batches: Vec<RecordBatch> = Vec::new();
    for file in list_files(output)
        .await
        .map_err(|error| error.to_string())?
        .into_iter()
        .filter(|file| file.relative_key.ends_with(".parquet"))
    {
        let bytes = read_file(output, &file.relative_key)
            .await
            .map_err(|error| error.to_string())?;
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
            .and_then(|builder| builder.build())
            .map_err(|error| error.to_string())?;
        for batch in reader {
            batches.push(batch.map_err(|error| error.to_string())?);
        }
    }
    let loads: std::collections::BTreeSet<String> = column_values(&batches, "_sqlload_load_id")?
        .into_iter()
        .collect();
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    Ok(format!("{rows}|{}", loads.len()))
}

/// A record rewritten in the format SQLLoad wrote before 0.61.0: one list of `[file, version]` pairs.
pub(crate) fn old_record(body: &[u8]) -> Result<Vec<u8>, String> {
    let record: serde_json::Value =
        serde_json::from_slice(body).map_err(|error| error.to_string())?;
    let files: Vec<serde_json::Value> = record["commits"]
        .as_object()
        .into_iter()
        .flat_map(|commits| commits.values())
        .filter_map(serde_json::Value::as_object)
        .flat_map(|folders| folders.values())
        .filter_map(serde_json::Value::as_array)
        .flatten()
        .cloned()
        .collect();
    serde_json::to_vec(&files).map_err(|error| error.to_string())
}

/// A record left as it is.
pub(crate) fn same_record(body: &[u8]) -> Result<Vec<u8>, String> {
    Ok(body.to_vec())
}

/// Load two days from a files source into a local files target; tamper with the target's folder as the case says and add a third day; plan the next load, let the case's interleaved loads commit, then run the plan. The second load's error, if any, and the target's `rows|loads records:<count> markers:<name>=<phase>,...`.
pub(crate) async fn run_planted(
    directory: &std::path::Path,
    test_case: &PlantedTestCase,
) -> Result<(Option<String>, String), String> {
    let (store, connection) = local_store("", directory);
    let folder = |prefix: &str| {
        open_folder(&Location {
            store: store.clone(),
            prefix: prefix.to_owned(),
        })
        .map_err(|error| error.to_string())
    };
    let (source, table) = (folder("src/")?, folder("out/orders/")?);
    let text = format!(
        "[connections.files]\n{connection}\n\n[targets.lake]\nconnection = \"files\"\npath = \"out/{{table}}/\"\n\n[sources.orders]\nkind = \"files\"\ndescription = \"Test table.\"\nconnection = \"files\"\npath = \"src/\"\npartition_columns = {{ day = \"date\" }}\nwrite_strategy = \"{}\"\n",
        test_case.strategy
    );
    let failed = |error: sqlload::file_store::errors::StoreError| error.to_string();
    let project = described_project(&text).await?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let add = async |files: Vec<(String, bytes::Bytes)>| {
        for (key, body) in files {
            write_file(&source, &key, body).await.map_err(failed)?;
        }
        Ok::<(), String>(())
    };
    add(two_days()?).await?;
    run_once(&text, &destinations, &PlanOptions::default()).await?;
    let records = folder("out/orders/_sqlload/loaded/")?;
    for record in list_files(&records).await.map_err(failed)? {
        let body = read_file(&records, &record.relative_key)
            .await
            .map_err(failed)?;
        let rewritten = (test_case.rewrite)(&body)?;
        write_file(&records, &record.relative_key, rewritten.into())
            .await
            .map_err(failed)?;
    }
    for (key, body) in test_case.planted {
        let body = body.replace("SOURCE", &source.location.to_string());
        write_file(&table, key, body.into()).await.map_err(failed)?;
    }
    add(third_day()?).await?;
    let options = PlanOptions {
        reload: test_case.reload,
        ..PlanOptions::default()
    };
    let planned = plan_project(&project, &destinations, &options)
        .await
        .map_err(|error| error.to_string())?;
    for _ in 0..test_case.interleaved {
        run_once(&text, &destinations, &PlanOptions::default()).await?;
    }
    let error = async {
        let (lines, ok) = render_plan(&planned, true);
        ok.then_some(()).ok_or_else(|| lines.join("\n"))?;
        apply_project(&planned, &destinations)
            .await
            .map_err(|error| error.to_string())?;
        load_with_options(&project, &planned, &destinations, &options).await
    }
    .await
    .err();
    let mut marked: Vec<String> = Vec::new();
    for kind in ["pending", "decided"] {
        let markers = folder(&format!("out/orders/_sqlload/{kind}/"))?;
        for marker in list_files(&markers).await.map_err(failed)? {
            let body = read_file(&markers, &marker.relative_key)
                .await
                .map_err(failed)?;
            let phase = String::from_utf8_lossy(&body)
                .lines()
                .next()
                .unwrap_or_default()
                .to_owned();
            marked.push(format!("{kind}/{}={phase}", marker.relative_key));
        }
    }
    let count = list_files(&records).await.map_err(failed)?.len();
    let held = held_rows(&folder("out/")?).await?;
    Ok((
        error,
        format!("{held} records:{count} markers:{}", marked.join(",")),
    ))
}

/// Load a files table into DuckDB; take `_sqlload_source_row` away, as in a table from before it; add a file and load again. Each row's id and row number.
pub(crate) async fn run_source_row_upgrade(
    directory: &std::path::Path,
    test_case: &SourceRowUpgradeTestCase,
) -> Result<Option<String>, String> {
    let (store, connection) = local_store("", directory);
    let folder = open_folder(&Location {
        store,
        prefix: "scores/".to_owned(),
    })
    .map_err(|error| error.to_string())?;
    let text = format!(
        "[connections.files]\n{connection}\n\n[connections.local]\nkind = \"duckdb\"\ndatabase = \":memory:\"\n\n[targets.dev]\nconnection = \"local\"\nloader_schema = \"raw\"\n\n[sources.scores]\nkind = \"files\"\ndescription = \"Test table.\"\nconnection = \"files\"\npath = \"scores/\"\nwrite_strategy = \"append\"\n{}\n",
        test_case.settings
    );
    let project_destinations = connect_targets(&described_project(&text).await?)
        .await
        .map_err(|error| error.to_string())?;
    let Some(duckdb @ Destination::DuckDb(_)) = project_destinations.get("dev") else {
        return Err("no DuckDB destination".to_owned());
    };
    for (key, ids) in [("a.parquet", &[1_i64, 2][..]), ("b.parquet", &[3][..])] {
        let file = ParquetFile {
            key,
            ids,
            score: 1.0,
            extra: None,
        };
        let written = [key]
            .into_iter()
            .filter(|key| *key == "a.parquet" || test_case.new_file);
        for key in written {
            write_file(&folder, key, parquet_bytes(&file)?)
                .await
                .map_err(|error| error.to_string())?;
        }
        run_once(&text, &project_destinations, &PlanOptions::default()).await?;
        execute(
            duckdb,
            [(
                "a.parquet",
                "ALTER TABLE raw.scores DROP COLUMN _sqlload_source_row",
            )]
            .iter()
            .filter(|(when, _)| *when == key)
            .map(|(_, statement)| (*statement).to_owned())
            .collect(),
        )
        .await
        .map_err(|error| error.to_string())?;
    }
    single_text(
        duckdb,
        "SELECT string_agg(id || ':' || coalesce(CAST(_sqlload_source_row AS VARCHAR), '-'), ',' ORDER BY id) FROM raw.scores".to_owned(),
    )
    .await
    .map_err(|error| error.to_string())
}

/// An xlsx workbook of one sheet: a row per line of `text`, cells separated by `|`, each written as text.
pub(crate) fn workbook(text: &str) -> Result<Vec<u8>, String> {
    let failed = |error: rust_xlsxwriter::XlsxError| error.to_string();
    let mut book = rust_xlsxwriter::Workbook::new();
    let sheet = book.add_worksheet();
    for (row, line) in text.lines().enumerate() {
        let row = u32::try_from(row).map_err(|error| error.to_string())?;
        for (column, cell) in line.split('|').enumerate() {
            let column = u16::try_from(column).map_err(|error| error.to_string())?;
            sheet.write_string(row, column, cell).map_err(failed)?;
        }
    }
    book.save_to_buffer().map_err(failed)
}

/// `text` as a file's bytes, unchanged.
pub(crate) fn text_bytes(text: &str) -> Result<Vec<u8>, String> {
    Ok(text.as_bytes().to_vec())
}

/// Plan and load `test_case`'s runs with the executable into a DuckDB file, each run adding its file: whether each run planned and loaded and its plan held the expected text, then the table's columns and what the check query returned, with every run's output.
pub(crate) fn names_case_outcome(test_case: &ColumnNamesTestCase) -> Result<NamesOutcome, String> {
    let root = tempfile::tempdir().map_err(|error| error.to_string())?;
    std::fs::create_dir_all(root.path().join("drop")).map_err(|error| error.to_string())?;
    let database = root.path().join("out.duckdb");
    let config = root.path().join("sqlload.toml");
    let text = format!(
        "[connections.wh]\nkind = \"duckdb\"\ndatabase = \"{}\"\n\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"raw\"\n\n[connections.files]\nkind = \"filesystem\"\nroot = \"{}\"\n\n[sources.ShopOrders]\nkind = \"files\"\ndescription = \"Test table.\"\nconnection = \"files\"\npath = \"drop/\"\n{}",
        database.display(),
        root.path().display(),
        test_case.settings
    );
    std::fs::write(&config, text).map_err(|error| error.to_string())?;
    let mut outcome = NamesOutcome::default();
    for run in test_case.runs {
        std::fs::write(
            root.path().join("drop").join(run.file),
            (run.encode)(run.contents)?,
        )
        .map_err(|error| error.to_string())?;
        let (planned, plan) = run_sqlload(&config, &["plan", "-v"], ("", ""))?;
        let (loaded, load) = run_sqlload(&config, &["load"], ("", ""))?;
        let shown = words(&plan).contains(&words(run.expected_plan));
        outcome.runs.push((planned, loaded, shown));
        outcome.output.push_str(&format!("{plan}\n{load}\n"));
    }
    let (table, _) = test_case.expected_columns;
    outcome.columns = duckdb_text(
        &database,
        &format!(
            "SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'raw' AND table_name = '{table}' AND NOT starts_with(column_name, '_')"
        ),
    )?;
    outcome.checked = duckdb_text(&database, test_case.check.0)?;
    Ok(outcome)
}
