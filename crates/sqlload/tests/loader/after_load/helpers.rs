use sqlload::file_store::main::list_files::list_files;
use sqlload::file_store::main::open_folder::open_folder;
use sqlload::file_store::main::read_file::read_file;
use sqlload::file_store::main::write_file::write_file;
use sqlload::files_source::main::after_load::after_load;
use sqlload::store_paths::models::{Location, StoreKind};
use sqlload_testkit::seaweedfs::constants::REGION;

use crate::test_types::{AfterLoadTestCase, S3AfterLoadTestCase};

/// Run `test_case`'s steps through the `sqlload` executable (`load --json`), asserting each step's exit code and report; returns the files left below the root as `path=contents`, sorted.
pub(crate) async fn run_after_load_case(
    test_case: &AfterLoadTestCase,
) -> Result<Vec<String>, String> {
    let root = tempfile::tempdir().map_err(|error| error.to_string())?;
    let work = tempfile::tempdir().map_err(|error| error.to_string())?;
    let mut elsewhere: Vec<tempfile::TempDir> = Vec::new();
    for (folder, parent) in test_case.linked {
        let target = tempfile::tempdir_in(parent).map_err(|error| error.to_string())?;
        std::os::unix::fs::symlink(target.path(), root.path().join(folder))
            .map_err(|error| error.to_string())?;
        elsewhere.push(target);
    }
    let config = work.path().join("sqlload.toml");
    let text = format!(
        "[connections.wh]\nkind = \"duckdb\"\ndatabase = \"{}\"\n\n[connections.files]\nkind = \"filesystem\"\nroot = \"{}\"\n\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"raw\"\n\n[sources.scores]\nkind = \"files\"\ndescription = \"Test table.\"\nconnection = \"files\"\npath = \"drop/\"\nformat = \"csv\"\n{}\n",
        work.path().join("out.duckdb").display(),
        root.path().display(),
        test_case.settings
    );
    std::fs::write(&config, text).map_err(|error| error.to_string())?;
    for (index, step) in test_case.steps.iter().enumerate() {
        for (path, contents) in step.files {
            let file = root.path().join(path);
            std::fs::create_dir_all(file.parent().unwrap_or(root.path()))
                .map_err(|error| error.to_string())?;
            std::fs::write(&file, contents).map_err(|error| error.to_string())?;
        }
        for folder in step.removed {
            std::fs::remove_dir_all(root.path().join(folder)).map_err(|error| error.to_string())?;
        }
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_sqlload"))
            .arg("--config")
            .arg(&config)
            .arg("load")
            .arg("--json")
            .output()
            .map_err(|error| error.to_string())?;
        let report: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|error| format!("{error}: {}", String::from_utf8_lossy(&output.stdout)))?;
        let table = &report["tables"][0];
        let mut after_load = table["after_load"].clone();
        let failed: Vec<serde_json::Value> = after_load["failed"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_default()
            .iter()
            .map(|failure| {
                serde_json::Value::from(
                    failure["file"]
                        .as_str()
                        .unwrap_or_default()
                        .replace(&root.path().display().to_string(), "<root>"),
                )
            })
            .collect();
        after_load
            .as_object_mut()
            .map(|object| object.insert("failed".to_owned(), failed.into()));
        let skipped: Vec<serde_json::Value> = report["plan"]["skipped"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_default()
            .iter()
            .map(|item| item["details"][0]["text"].clone())
            .collect();
        let problems: Vec<serde_json::Value> = report["plan"]["errors"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_default()
            .iter()
            .map(|item| {
                serde_json::Value::from(
                    item["details"][0]["text"]
                        .as_str()
                        .unwrap_or_default()
                        .replace(&root.path().display().to_string(), "<root>"),
                )
            })
            .collect();
        let shown = serde_json::json!({
            "problems": problems,
            "status": table["status"],
            "rows": table["rows"],
            "after_load": after_load,
            "skipped": skipped,
        });
        assert_eq!(
            (output.status.code(), shown.to_string()),
            (Some(step.expected_exit), step.expected_report.to_owned()),
            "{} step {}: {report}",
            test_case.description,
            index + 1
        );
    }
    let folder = open_folder(&Location {
        store: StoreKind::Filesystem {
            root: root.path().display().to_string(),
        },
        prefix: String::new(),
    })
    .map_err(|error| error.to_string())?;
    let mut files: Vec<String> = Vec::new();
    for file in list_files(&folder)
        .await
        .map_err(|error| error.to_string())?
    {
        let contents = read_file(&folder, &file.relative_key)
            .await
            .map_err(|error| error.to_string())?;
        files.push(format!(
            "{}={}",
            file.relative_key,
            String::from_utf8_lossy(&contents).replace('\n', "\\n")
        ));
    }
    files.sort();
    Ok(files)
}

/// Write `test_case`'s files into its folder of `bucket` at `endpoint`, list the source's `drop/` files, rewrite its `rewritten` ones, run its `after_load` on the listed files, and return the report as `action status files failed` and every file left in its folder as `key=contents`, sorted.
pub(crate) async fn run_s3_after_load(
    (endpoint, bucket): (&str, &str),
    test_case: &S3AfterLoadTestCase,
) -> Result<(String, Vec<String>), String> {
    let store = StoreKind::S3 {
        bucket: bucket.to_owned(),
        region: Some(REGION.to_owned()),
        endpoint: Some(endpoint.to_owned()),
        profile: None,
    };
    let open = |prefix: String| {
        open_folder(&Location {
            store: store.clone(),
            prefix,
        })
        .map_err(|error| error.to_string())
    };
    let case = open(test_case.folder.to_owned())?;
    for (key, text) in test_case.files {
        write_file(&case, key, bytes::Bytes::from(*text))
            .await
            .map_err(|error| error.to_string())?;
    }
    let source = open(format!("{}drop/", test_case.folder))?;
    let listed = list_files(&source)
        .await
        .map_err(|error| error.to_string())?;
    for (key, text) in test_case.rewritten {
        write_file(&case, key, bytes::Bytes::from(*text))
            .await
            .map_err(|error| error.to_string())?;
    }
    let report = after_load(&source, &test_case.after_load, &listed)
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
    let mut files: Vec<String> = Vec::new();
    for file in list_files(&case).await.map_err(|error| error.to_string())? {
        let contents = read_file(&case, &file.relative_key)
            .await
            .map_err(|error| error.to_string())?;
        files.push(format!(
            "{}={}",
            file.relative_key,
            String::from_utf8_lossy(&contents)
        ));
    }
    files.sort();
    Ok((report, files))
}
