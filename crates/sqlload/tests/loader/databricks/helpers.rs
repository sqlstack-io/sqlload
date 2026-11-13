//! Helpers for the Databricks loader tests: the fake workspace's scripted answers and the live workspace.

use sqlload::adapters::dispatch::main::session::single_text::single_text;
use sqlload::file_store::main::open_folder::open_folder;
use sqlload::file_store::main::write_file::write_file;
use sqlload::loader::main::connect_targets::connect_targets;
use sqlload::loader::models::PlanOptions;
use sqlload::store_paths::models::{Location, StoreKind};

use crate::helpers::{described_project, plan_apply_load};
use crate::test_types::{
    DatabricksAnswerTestCase, DatabricksLoadTestCase, DatabricksOutcome, LiveDatabricks,
    LiveDatabricksTestCase,
};

pub(crate) fn nothing_in_databricks(_: &sqlload_testkit::fake_databricks::models::FakeDatabricks) {}

pub(crate) fn copy_fails(fake: &sqlload_testkit::fake_databricks::models::FakeDatabricks) {
    fake.state
        .lock()
        .copy_failures
        .push_back("[COPY_INTO_SOURCE_SCHEMA_MISMATCH] staged file refused".to_owned());
}

pub(crate) fn expired_token(fake: &sqlload_testkit::fake_databricks::models::FakeDatabricks) {
    fake.state.lock().refuse_requests = 1;
}

/// A failed run's folder two days old, a folder another load is still filling, an empty folder a failed run left, and an empty folder a starting load writes its first file into just after cleanup lists it.
pub(crate) fn stale_and_running_loads(
    fake: &sqlload_testkit::fake_databricks::models::FakeDatabricks,
) {
    let now_ms = chrono::Utc::now().timestamp_millis();
    fake.place_file(
        "/Volumes/main/raw/stage/sqlload/old-load/00001/part-00001.parquet",
        b"PAR1",
        now_ms - 2 * 86_400_000,
    );
    fake.place_file(
        "/Volumes/main/raw/stage/sqlload/running-load/00001/part-00001.parquet",
        b"PAR1",
        now_ms,
    );
    fake.place_folder("/Volumes/main/raw/stage/sqlload/abandoned-load/00001");
    fake.place_folder("/Volumes/main/raw/stage/sqlload/starting-load/00001");
    fake.state.lock().write_after_listing = Some((
        "/Volumes/main/raw/stage/sqlload/starting-load/00001".to_owned(),
        "/Volumes/main/raw/stage/sqlload/starting-load/00001/part-00001.parquet".to_owned(),
    ));
}

/// The stale and running load folders, with the warehouse answering every statement after a poll (the first poll failing with 503), results one row per chunk, and listings one entry per page.
pub(crate) fn slow_paged_answers(fake: &sqlload_testkit::fake_databricks::models::FakeDatabricks) {
    stale_and_running_loads(fake);
    let mut state = fake.state.lock();
    state.pending_polls = 1;
    state.poll_failures.push_back(503);
    state.chunk_rows = Some(1);
    state.page_entries = Some(1);
}

/// Every statement answers after a poll, and the first poll is refused outright.
pub(crate) fn poll_refused(fake: &sqlload_testkit::fake_databricks::models::FakeDatabricks) {
    let mut state = fake.state.lock();
    state.pending_polls = 1;
    state.poll_failures.push_back(400);
}

/// Create a three-column table on a fresh fake delivering answers as the case says, then read its columns back through the client.
pub(crate) async fn databricks_columns(
    test_case: &DatabricksAnswerTestCase,
) -> Result<Vec<Option<String>>, String> {
    let fake =
        sqlload_testkit::fake_databricks::main::start_fake_databricks::start_fake_databricks()
            .await?;
    {
        let mut state = fake.state.lock();
        state.chunk_rows = test_case.chunk_rows;
        state.pending_polls = test_case.pending_polls;
    }
    let http = sqlload::http::main::http_client::build_http_client(
        &sqlload::http::models::ConnectionCounter::default(),
    )
    .map_err(|error| error.to_string())?;
    let api = sqlload::databricks_api::models::DatabricksClient::new(
        http,
        &fake.base_url,
        "wh",
        ("id", sqlload::auth::models::Secret("secret".to_owned())),
    );
    let execute = sqlload::databricks_api::main::execute_statement::execute_statement;
    execute(&api, "CREATE TABLE IF NOT EXISTS `main`.`raw`.`orders` (`id` BIGINT, `amount` decimal(12,2), `note` STRING)")
        .await
        .map_err(|error| error.to_string())?;
    let rows = execute(&api, "SELECT column_name, full_data_type FROM `main`.information_schema.columns WHERE table_schema = 'raw' AND table_name = 'orders'")
        .await
        .map_err(|error| error.to_string())?;
    Ok(rows
        .rows
        .into_iter()
        .map(|row| row.into_iter().next().flatten())
        .collect())
}

/// Load the case's files into a fresh fake Databricks workspace; what was planned and sent, and what the volume still holds.
pub(crate) async fn run_databricks(
    directory: &std::path::Path,
    test_case: &DatabricksLoadTestCase,
) -> Result<DatabricksOutcome, String> {
    use sqlload_testkit::fake_databricks::constants::{FAKE_CATALOG, FAKE_VOLUME};
    let fake =
        sqlload_testkit::fake_databricks::main::start_fake_databricks::start_fake_databricks()
            .await?;
    (test_case.prepare)(&fake);
    let root = directory.display().to_string();
    let source = open_folder(&Location {
        store: StoreKind::Filesystem { root: root.clone() },
        prefix: "src/".to_owned(),
    })
    .map_err(|error| error.to_string())?;
    for (key, body) in (test_case.file_set)()? {
        write_file(&source, &key, body)
            .await
            .map_err(|error| error.to_string())?;
    }
    let text = format!(
        "[connections.dbx]\nkind = \"databricks\"\nhost = \"{}\"\nclient_id = \"id\"\nclient_secret = \"secret\"\nwarehouse_id = \"wh\"\ncatalog = \"{FAKE_CATALOG}\"\nvolume = \"{FAKE_VOLUME}\"\n\n[connections.lake]\nkind = \"filesystem\"\nroot = \"{root}\"\n\n[targets.dev]\nconnection = \"dbx\"\nloader_schema = \"raw\"\n\n[sources.orders]\nkind = \"files\"\ndescription = \"Test table.\"\nconnection = \"lake\"\npath = \"src/\"\npartition_columns = {{ day = \"date\" }}\n{}\n",
        fake.base_url, test_case.settings
    );
    let project = described_project(&text).await?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let (plan, outcome) = plan_apply_load(&project, &destinations, &PlanOptions::default()).await;
    let left = fake.volume_leftovers();
    let rows = fake.rows("raw.orders").len();
    let state = fake.state.lock();
    Ok(DatabricksOutcome {
        error: outcome.err().unwrap_or_default(),
        plan,
        sent: state.statements.clone(),
        rows,
        copies: state.copied.len(),
        left,
        cancelled: state.cancelled.len(),
    })
}

/// The workspace the live Databricks test loads into; `Ok(None)` when `SQLLOAD_DATABRICKS_SCHEMA` is unset, which skips the test.
pub(crate) fn live_databricks() -> Result<Option<LiveDatabricks>, String> {
    let Ok(schema) = std::env::var("SQLLOAD_DATABRICKS_SCHEMA") else {
        return Ok(None);
    };
    schema.starts_with("sqlload_").then_some(()).ok_or_else(|| format!(
        "SQLLOAD_DATABRICKS_SCHEMA must start with sqlload_, the only schemas this test may create and drop, not {schema:?}"
    ))?;
    let required = |name: &str| std::env::var(name).map_err(|_| format!("{name} is not set"));
    Ok(Some(LiveDatabricks {
        host: required("DATABRICKS_HOST")?,
        client_id: required("DATABRICKS_CLIENT_ID")?,
        client_secret: required("DATABRICKS_CLIENT_SECRET")?,
        warehouse_id: required("DATABRICKS_WAREHOUSE_ID")?,
        catalog: std::env::var("SQLLOAD_DATABRICKS_CATALOG")
            .unwrap_or_else(|_| "workspace".to_owned()),
        schema,
    }))
}

/// A client for the live workspace's warehouse.
pub(crate) fn live_databricks_client(
    live: &LiveDatabricks,
) -> Result<sqlload::databricks_api::models::DatabricksClient, String> {
    Ok(sqlload::databricks_api::models::DatabricksClient::new(
        sqlload::http::main::http_client::build_http_client(
            &sqlload::http::models::ConnectionCounter::default(),
        )
        .map_err(|error| error.to_string())?,
        &live.host,
        &live.warehouse_id,
        (
            &live.client_id,
            sqlload::auth::models::Secret(live.client_secret.clone()),
        ),
    ))
}

/// Drop the live `sqlload_` schema, then create it and its staging volume again empty.
pub(crate) async fn reset_live_databricks(
    api: &sqlload::databricks_api::models::DatabricksClient,
    live: &LiveDatabricks,
) -> Result<(), String> {
    let schema = format!("`{}`.`{}`", live.catalog, live.schema);
    for statement in [
        format!("DROP SCHEMA IF EXISTS {schema} CASCADE"),
        format!("CREATE SCHEMA {schema}"),
        format!("CREATE VOLUME IF NOT EXISTS {schema}.`sqlload_stage`"),
    ] {
        sqlload::databricks_api::main::execute_statement::execute_statement(api, &statement)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

/// Load `load`'s files (written once, so a second load of the same source finds them unchanged) into the live workspace; the load's error (empty when it succeeded), the table's row count, and the load folders left in the staging volume.
pub(crate) async fn run_live_databricks(
    directory: &std::path::Path,
    live: &LiveDatabricks,
    load: &LiveDatabricksTestCase,
) -> Result<(String, Option<String>, usize), String> {
    let root = directory.display().to_string();
    let source = open_folder(&Location {
        store: StoreKind::Filesystem { root: root.clone() },
        prefix: format!("{}/", load.source),
    })
    .map_err(|error| error.to_string())?;
    let written = directory.join(load.source).exists();
    for (key, body) in (load.file_set)()?.into_iter().filter(|_| !written) {
        write_file(&source, &key, body)
            .await
            .map_err(|error| error.to_string())?;
    }
    let LiveDatabricks {
        host,
        client_id,
        client_secret,
        warehouse_id,
        catalog,
        schema,
    } = live;
    let text = format!(
        "[connections.dbx]\nkind = \"databricks\"\nhost = \"{host}\"\nclient_id = \"{client_id}\"\nclient_secret = \"{client_secret}\"\nwarehouse_id = \"{warehouse_id}\"\ncatalog = \"{catalog}\"\nvolume = \"{catalog}.{schema}.sqlload_stage\"\n\n[connections.lake]\nkind = \"filesystem\"\nroot = \"{root}\"\n\n[targets.dev]\nconnection = \"dbx\"\nloader_schema = \"{schema}\"\n\n[sources.{}]\nkind = \"files\"\ndescription = \"Test table.\"\nconnection = \"lake\"\npath = \"{}/\"\npartition_columns = {{ day = \"date\" }}\n{}\n",
        load.source, load.source, load.settings
    );
    let project = described_project(&text).await?;
    let destinations = connect_targets(&project)
        .await
        .map_err(|error| error.to_string())?;
    let (_, outcome) = plan_apply_load(&project, &destinations, &PlanOptions::default()).await;
    let destination = destinations
        .get("dev")
        .ok_or_else(|| "no dev target".to_owned())?;
    let rows = single_text(
        destination,
        format!(
            "SELECT CAST(COUNT(*) AS STRING) FROM `{catalog}`.`{schema}`.`{}`",
            load.source
        ),
    )
    .await
    .map_err(|error| error.to_string())?;
    let api = sqlload::databricks_api::models::DatabricksClient::new(
        sqlload::http::main::http_client::build_http_client(
            &sqlload::http::models::ConnectionCounter::default(),
        )
        .map_err(|error| error.to_string())?,
        host,
        warehouse_id,
        (
            client_id,
            sqlload::auth::models::Secret(client_secret.clone()),
        ),
    );
    let folders = sqlload::databricks_api::main::list_directory::list_directory(
        &api,
        &format!("/Volumes/{catalog}/{schema}/sqlload_stage/sqlload"),
    )
    .await
    .map_err(|error| error.to_string())?
    .unwrap_or_default()
    .len();
    Ok((outcome.err().unwrap_or_default(), rows, folders))
}

/// One day's NDJSON orders whose `items` lists, with a list in each item, make child tables.
pub(crate) fn listed_items() -> Result<Vec<(String, bytes::Bytes)>, String> {
    Ok(vec![(
        "day=2026-11-01/a.ndjson".to_owned(),
        bytes::Bytes::from_static(
            b"{\"id\": 1, \"items\": [{\"sku\": \"a\", \"parts\": [1, 2]}]}\n{\"id\": 2, \"items\": [{\"sku\": \"b\", \"parts\": [3]}]}\n",
        ),
    )])
}

/// A child table an earlier load left for the camelCase field `lineItems`, listed lower-cased as Unity Catalog stores table names.
pub(crate) fn lowercased_child_table(
    fake: &sqlload_testkit::fake_databricks::models::FakeDatabricks,
) {
    fake.state.lock().tables.insert(
        "raw.orders__lineitems".to_owned(),
        sqlload_testkit::fake_databricks::models::FakeTable {
            columns: vec![
                ("sku".to_owned(), "string".to_owned()),
                ("_sqlload_parent_id".to_owned(), "string".to_owned()),
            ],
            rows: Vec::new(),
        },
    );
}

/// Orders with a camelCase list field.
pub(crate) fn camel_items() -> Result<Vec<(String, bytes::Bytes)>, String> {
    Ok(vec![(
        "day=2026-11-01/a.ndjson".to_owned(),
        bytes::Bytes::from_static(
            b"{\"id\": 1, \"lineItems\": [{\"sku\": \"a\"}]}\n{\"id\": 2, \"lineItems\": [{\"sku\": \"b\"}]}\n",
        ),
    )])
}
