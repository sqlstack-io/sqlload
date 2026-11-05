use sqlload::file_store::main::delete_files::delete_files;
use sqlload::file_store::main::list_files::list_files;
use sqlload::file_store::main::open_folder::open_folder;
use sqlload::file_store::main::write_file::write_file;
use sqlload::file_store::models::OpenFolder;
use sqlload::loader::main::apply_project::apply_project;
use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::main::load_tables::load_tables;
use sqlload::loader::main::plan_project::plan_project;
use sqlload::loader::main::render_plan::render_plan;
use sqlload::loader::models::{Destination, LoadRun, PlanOptions};
use sqlload::project::main::parse_project::parse_project;
use sqlload::snowflake_destination::main::query_texts::query_texts;
use sqlload::store_paths::models::{Location, StoreKind};

use crate::parity::_helpers::files::{case_files, parquet_file};
use crate::parity::models::{Live, ParityCase, Side};

/// The project text holds every value itself, so nothing is read from the environment.
fn no_variables(_: &str) -> Option<String> {
    None
}

fn project_text(live: &Live, folder: &str, test_case: &ParityCase, tables: (&str, &str)) -> String {
    let connection = |name: &str, stage: &str| {
        format!(
            "[connections.{name}]\nkind = \"snowflake\"\naccount = \"{}\"\nuser = \"{}\"\nauth = {}\nrole = \"{}\"\nwarehouse = \"{}\"\ndatabase = \"{}\"\nstage = {stage}\n\n[targets.{name}]\nconnection = \"{name}\"\nloader_schema = \"{}\"\n\n",
            live.account,
            live.user,
            live.auth,
            live.role,
            live.warehouse,
            live.database,
            live.schema
        )
    };
    let source = |name: &str, table: &str, side: &Side| {
        format!(
            "[sources.{name}]\nkind = \"files\"\nconnection = \"lake\"\npath = \"{folder}\"\ndestination = \"{}\"\ntable_name = \"{table}\"\npartition_columns = {{ {} }}\n{}\n\n",
            side.target,
            test_case.partition_columns,
            side.settings.replace("SOURCE", name)
        )
    };
    format!(
        "{}{}{}[connections.lake]\nkind = \"s3\"\nbucket = \"{}\"\nregion = \"{}\"\n\n{}{}",
        connection(
            "direct",
            &format!(
                "{{ kind = \"s3\", url = \"s3://{}/{}\", storage_integration = \"{}\", region = \"{}\" }}",
                live.bucket, live.prefix, live.integration, live.region
            )
        ),
        connection("streaming", "{ kind = \"streaming\" }"),
        connection("insert", "{ kind = \"insert\" }"),
        live.bucket,
        live.region,
        source("left", tables.0, &test_case.left),
        source("right", tables.1, &test_case.right),
    )
}

/// Load one side; its plan, and the load's failure if any.
async fn load_side(text: &str, select: &str) -> Result<(String, Option<String>), String> {
    let project = parse_project(text, &no_variables).map_err(|error| error.to_string())?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let options = PlanOptions {
        select: vec![select.to_owned()],
        ..PlanOptions::default()
    };
    let plan = match plan_project(&project, &destinations, &options).await {
        Ok(plan) => plan,
        Err(error) => return Ok((String::new(), Some(error.to_string()))),
    };
    let (lines, ok) = render_plan(&plan);
    let plan_text = lines.join("\n");
    if !ok {
        return Ok((plan_text.clone(), Some(plan_text)));
    }
    if let Err(error) = apply_project(&plan, &destinations).await {
        return Ok((plan_text, Some(error.to_string())));
    }
    let run = LoadRun {
        project: &project,
        destinations: &destinations,
        host: None,
        options: &options,
    };
    let failure = load_tables(&run, &plan)
        .await
        .into_iter()
        .find_map(|outcome| outcome.loaded.err());
    Ok((plan_text, failure))
}

async fn lake_folder(live: &Live, prefix: &str) -> Result<OpenFolder, String> {
    open_folder(&Location {
        store: StoreKind::S3 {
            bucket: live.bucket.clone(),
            region: Some(live.region.clone()),
            endpoint: None,
        },
        prefix: prefix.to_owned(),
    })
    .map_err(|error| error.to_string())
}

/// The rows of one table but not the other, as text, for `SELECT` columns but this load's id and time.
async fn differences(text: &str, live: &Live, tables: (&str, &str)) -> Result<Vec<String>, String> {
    let project = parse_project(text, &no_variables).map_err(|error| error.to_string())?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let Some(Destination::Snowflake(snowflake)) = destinations.get("direct") else {
        return Err("no Snowflake connection".to_owned());
    };
    let columns = query_texts(
        snowflake,
        &format!(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}' AND COLUMN_NAME NOT IN ('_SQLLOAD_LOAD_ID', '_SQLLOAD_LOADED_AT') ORDER BY COLUMN_NAME",
            live.schema, tables.0
        ),
    )
    .await
    .map_err(|error| error.to_string())?;
    let select: Vec<String> = columns
        .iter()
        .map(|column| format!("COALESCE(TO_VARCHAR(\"{column}\"), '<NULL>')"))
        .collect();
    let row = select.join(" || '|' || ");
    let table = |name: &str| format!("\"{}\".\"{name}\"", live.schema);
    let query = format!(
        "SELECT 'left only: ' || r FROM (SELECT {row} AS r FROM {0} MINUS SELECT {row} FROM {1}) UNION ALL SELECT 'right only: ' || r FROM (SELECT {row} AS r FROM {1} MINUS SELECT {row} FROM {0}) UNION ALL SELECT 'left has ' || (SELECT COUNT(*) FROM {0}) || ' rows, right has ' || (SELECT COUNT(*) FROM {1}) WHERE (SELECT COUNT(*) FROM {0}) <> (SELECT COUNT(*) FROM {1})",
        table(tables.0),
        table(tables.1)
    );
    query_texts(snowflake, &query)
        .await
        .map_err(|error| error.to_string())
}

/// Load the case both ways and compare: `same rows`, `both fail`, or what differed.
pub(crate) async fn run_case(live: &Live, test_case: &ParityCase) -> Result<String, String> {
    let id = uuid::Uuid::new_v4().simple().to_string();
    let short = id.get(..8).unwrap_or_default();
    let folder_key = format!("{}parity-{short}/{}/", live.prefix, test_case.name);
    let folder = lake_folder(live, &folder_key).await?;
    for (key, batch) in case_files(test_case.files)? {
        write_file(&folder, key, parquet_file(&batch)?)
            .await
            .map_err(|error| error.to_string())?;
    }
    let upper = test_case.name.to_ascii_uppercase();
    let tables = (
        format!("PARITY_{short}_{upper}_LEFT"),
        format!("PARITY_{short}_{upper}_RIGHT"),
    );
    let text = project_text(live, &folder_key, test_case, (&tables.0, &tables.1));
    let outcome = compare(&text, live, test_case, (&tables.0, &tables.1)).await;
    if let Ok(files) = list_files(&folder).await {
        let keys: Vec<String> = files.into_iter().map(|file| file.relative_key).collect();
        let _ = delete_files(&folder, &keys).await;
    }
    drop_tables(&text, live, (&tables.0, &tables.1)).await;
    outcome
}

async fn compare(
    text: &str,
    live: &Live,
    test_case: &ParityCase,
    tables: (&str, &str),
) -> Result<String, String> {
    let (left_plan, left) = load_side(text, "left").await?;
    let (_, right) = load_side(text, "right").await?;
    let direct = left_plan.contains("destination loads parquet files from");
    if test_case.left_is_direct && left.is_none() && !direct {
        return Ok(format!("the left side did not load directly:\n{left_plan}"));
    }
    match (left, right) {
        (Some(_), Some(_)) => Ok("both fail".to_owned()),
        (Some(reason), None) => Ok(format!("only the left side failed: {reason}")),
        (None, Some(reason)) => Ok(format!("only the right side failed: {reason}")),
        (None, None) => {
            let found = differences(text, live, tables).await?;
            Ok(match found.is_empty() {
                true => "same rows".to_owned(),
                false => found.join("\n"),
            })
        }
    }
}

async fn drop_tables(text: &str, live: &Live, tables: (&str, &str)) {
    let Ok(project) = parse_project(text, &no_variables) else {
        return;
    };
    let Ok(destinations) = connect_targets(&project).await else {
        return;
    };
    let Some(Destination::Snowflake(snowflake)) = destinations.get("direct") else {
        return;
    };
    for table in [tables.0, tables.1] {
        let statement = format!("DROP TABLE IF EXISTS \"{}\".\"{table}\"", live.schema);
        let _ = query_texts(snowflake, &statement).await;
    }
}
