use sqlload_testkit::fake_snowflake::models::FakeSnowflake;

use crate::helpers::{binary_file, day_files, found, not_found, run_async, run_route};
use crate::test_types::{RouteFiles, RouteTestCase};

const MANAGED: &str = "{ kind = \"s3\", url = \"s3://fake-bucket/\", storage_integration = \"LAKE_INT\", region = \"us-east-1\", endpoint = \"FAKE\" }";
const KEEPS: &str = "{ kind = \"s3\", url = \"s3://fake-bucket/\", storage_integration = \"LAKE_INT\", region = \"us-east-1\", endpoint = \"FAKE\", keep_staged_files = true }";
const OWN_STAGE: &str = "COPY INTO \"DB\".\"RAW\".\"ORDERS\"";
const MANAGED_REF: &str = "FROM @\"DB\".\"RAW\".\"_SQLLOAD_STAGE\"";
const WRITES_PARQUET: &str = "write Parquet to";
const PULLS: &str = "destination loads parquet files from s3://fake-bucket/src/";

const APPEND: &str = "write_strategy = \"append\"";
const REPLACING: &str = "write_strategy = \"delete_insert\"\ncursor_column = \"day\"";

fn two_days() -> Result<Vec<(String, bytes::Bytes)>, String> {
    let mut files = day_files("2026-11-01", (2, 0), None)?;
    files.extend(day_files("2026-11-02", (1, 0), None)?);
    Ok(files)
}

/// Enough files for two batches.
fn two_batches() -> Result<Vec<(String, bytes::Bytes)>, String> {
    let mut files = day_files("2026-11-01", (1000, 0), None)?;
    files.extend(day_files("2026-11-02", (1, 0), None)?);
    Ok(files)
}

fn with_text() -> Result<Vec<(String, bytes::Bytes)>, String> {
    day_files("2026-11-01", (1, 0), Some("{\"a\": 1}"))
}

fn long_names() -> Result<Vec<(String, bytes::Bytes)>, String> {
    day_files("2026-11-01", (300, 1500), None)
}

/// A full first batch of plain files, then a batch holding one the direct `COPY` must not load; the newest files, which planning samples, are plain.
fn binary_later() -> Result<Vec<(String, bytes::Bytes)>, String> {
    let mut files = day_files("2026-11-01", (1000, 0), None)?;
    files.push(binary_file("day=2026-11-02/part-0.parquet")?);
    files.extend(day_files("2026-11-03", (25, 0), None)?);
    Ok(files)
}

fn nothing(_: &FakeSnowflake) {}

fn no_user_stage(fake: &FakeSnowflake) {
    fake.state.lock().denied_stages.push("@~".to_owned());
}

fn no_internal_stage(fake: &FakeSnowflake) {
    fake.state
        .lock()
        .denied_stages
        .extend(["@~".to_owned(), "%".to_owned()]);
}

fn no_stage_or_host(fake: &FakeSnowflake) {
    no_internal_stage(fake);
    fake.state.lock().streaming_unreachable = true;
}

fn no_stage_or_channel(fake: &FakeSnowflake) {
    no_internal_stage(fake);
    fake.state.lock().channels_refused = true;
}

fn integration(fake: &FakeSnowflake) {
    fake.state.lock().integrations.insert("LAKE_INT".to_owned());
}

/// The integration, and a load folder a failed run left long ago.
fn old_leftovers(fake: &FakeSnowflake) {
    integration(fake);
    let mut state = fake.state.lock();
    state.objects_modified = Some("2020-01-01T00:00:00.000Z".to_owned());
    state.objects.insert(
        "fake-bucket/_sqlload/old-load/00001/part-00001.parquet".to_owned(),
        b"PAR1".to_vec(),
    );
}

const BASE: RouteTestCase = RouteTestCase {
    description: "",
    stage: "",
    settings: APPEND,
    files: RouteFiles::Directory,
    file_set: two_days,
    prepare: nothing,
    expected_error: "",
    expected_plan: &[],
    expected_sent: &[],
    expected_unsent: &[],
    expected_fetches: None,
    expected_staged_left: 0,
};

#[test]
fn given_a_snowflake_target_when_planning_and_loading_then_rows_take_the_first_route_the_role_can_use()
-> Result<(), String> {
    let test_cases = [
        RouteTestCase {
            description: "no stage set: @~, one credentials fetch for the plan's probe and one for each of the two batches",
            file_set: two_batches,
            expected_plan: &["write Parquet to internal stage @~"],
            expected_sent: &[
                OWN_STAGE,
                "FROM @~/sqlload/",
                "REMOVE @~/sqlload/",
                "LIST @~/sqlload/",
            ],
            expected_fetches: Some(3),
            ..BASE
        },
        RouteTestCase {
            description: "@~ refused: an append into a table this load creates uses its table stage",
            prepare: no_user_stage,
            expected_plan: &[
                "@~ cannot be used",
                "write Parquet to internal stage @%table",
            ],
            expected_sent: &[
                OWN_STAGE,
                "FROM @\"DB\".\"RAW\".%\"ORDERS\"/sqlload/",
                "REMOVE @\"DB\".\"RAW\".%\"ORDERS\"/sqlload/",
            ],
            ..BASE
        },
        RouteTestCase {
            description: "@~ refused: a delete_insert uses the staging table's stage, after probing with a temporary table",
            settings: REPLACING,
            prepare: no_user_stage,
            expected_plan: &["the staging table's @%table"],
            expected_sent: &[
                "CREATE TEMPORARY TABLE IF NOT EXISTS",
                ".%\"ORDERS__SQLLOAD_STAGING_",
                "DELETE FROM \"DB\".\"RAW\".\"ORDERS\"",
            ],
            ..BASE
        },
        RouteTestCase {
            description: "no internal stage: Snowpipe Streaming into a staging table, then the usual write",
            settings: REPLACING,
            prepare: no_internal_stage,
            expected_plan: &[
                "@~ cannot be used",
                "@%table cannot be used",
                "Snowpipe Streaming into a transient staging table",
            ],
            expected_sent: &[
                "CREATE OR REPLACE TRANSIENT TABLE",
                "DELETE FROM \"DB\".\"RAW\".\"ORDERS\"",
            ],
            expected_unsent: &["COPY INTO", "FROM VALUES"],
            ..BASE
        },
        RouteTestCase {
            description: "an append streaming goes through staging too",
            prepare: no_internal_stage,
            expected_plan: &["Snowpipe Streaming into a transient staging table"],
            expected_sent: &["INSERT INTO \"DB\".\"RAW\".\"ORDERS\""],
            expected_unsent: &["COPY INTO"],
            ..BASE
        },
        RouteTestCase {
            description: "no stage and no streaming: batched INSERT into staging, warned in plan",
            prepare: no_stage_or_host,
            expected_plan: &[
                "batched INSERT into a transient staging table",
                "rows load by batched INSERT",
            ],
            expected_sent: &["FROM VALUES", "INSERT INTO \"DB\".\"RAW\".\"ORDERS\""],
            expected_unsent: &["COPY INTO"],
            ..BASE
        },
        RouteTestCase {
            description: "a chain-chosen stream that will not open drops to INSERT at load",
            prepare: no_stage_or_channel,
            expected_plan: &["Snowpipe Streaming into a transient staging table"],
            expected_sent: &["FROM VALUES"],
            ..BASE
        },
        RouteTestCase {
            description: "stage = user forces @~: refused, the plan fails and never falls back",
            stage: "{ kind = \"user\" }",
            prepare: no_user_stage,
            expected_error: "stage @~ cannot be used",
            expected_unsent: &["COPY INTO"],
            ..BASE
        },
        RouteTestCase {
            description: "stage = table forces @%table, past a working @~",
            stage: "{ kind = \"table\" }",
            expected_plan: &["internal stage @%table"],
            expected_sent: &["FROM @\"DB\".\"RAW\".%\"ORDERS\"/sqlload/"],
            ..BASE
        },
        RouteTestCase {
            description: "stage = streaming: its host unreachable fails the plan",
            stage: "{ kind = \"streaming\" }",
            prepare: no_stage_or_host,
            expected_error: "stage Snowpipe Streaming cannot be used",
            ..BASE
        },
        RouteTestCase {
            description: "stage = streaming: a channel that will not open fails the load, with no INSERT",
            stage: "{ kind = \"streaming\" }",
            prepare: no_stage_or_channel,
            expected_error: "Snowpipe Streaming will not open",
            expected_unsent: &["FROM VALUES"],
            ..BASE
        },
        RouteTestCase {
            description: "stage = insert goes straight to INSERT, with no probes",
            stage: "{ kind = \"insert\" }",
            expected_plan: &["batched INSERT"],
            expected_sent: &["FROM VALUES"],
            expected_fetches: Some(0),
            ..BASE
        },
        RouteTestCase {
            description: "a named internal stage",
            stage: "{ kind = \"internal\", name = \"DB.RAW.LOADS\" }",
            expected_plan: &["internal stage @DB.RAW.LOADS"],
            expected_sent: &[
                "FROM @DB.RAW.LOADS/sqlload/",
                "REMOVE @DB.RAW.LOADS/sqlload/",
            ],
            ..BASE
        },
        RouteTestCase {
            description: "a named internal stage refused fails the plan",
            stage: "{ kind = \"internal\", name = \"DB.RAW.LOADS\" }",
            prepare: |fake| fake.state.lock().denied_stages.push("LOADS".to_owned()),
            expected_error: "stage @DB.RAW.LOADS cannot be used",
            ..BASE
        },
        RouteTestCase {
            description: "the managed external stage: listed in plan, created by apply; SQLLoad writes the Parquet and removes it after each batch",
            stage: MANAGED,
            prepare: integration,
            expected_plan: &["create stage on s3://fake-bucket/", WRITES_PARQUET],
            expected_sent: &[
                "CREATE STAGE IF NOT EXISTS \"DB\".\"RAW\".\"_SQLLOAD_STAGE\"",
                MANAGED_REF,
            ],
            expected_unsent: &["REMOVE"],
            expected_fetches: Some(0),
            ..BASE
        },
        RouteTestCase {
            description: "keep_staged_files keeps both batches' files",
            file_set: two_batches,
            stage: KEEPS,
            prepare: integration,
            expected_staged_left: 2,
            ..BASE
        },
        RouteTestCase {
            description: "a load folder failed runs left over a day ago is removed at load start",
            stage: MANAGED,
            prepare: old_leftovers,
            expected_staged_left: 0,
            ..BASE
        },
        RouteTestCase {
            description: "keep_staged_files leaves that stage out of the cleanup",
            stage: KEEPS,
            prepare: old_leftovers,
            expected_staged_left: 2,
            ..BASE
        },
        RouteTestCase {
            description: "an external stage without its integration fails the plan, never falling back to @~",
            stage: MANAGED,
            expected_error: "storage integration LAKE_INT does not exist",
            expected_unsent: &["@~"],
            ..BASE
        },
        RouteTestCase {
            description: "files under the stage url load directly: nothing passes through SQLLoad",
            stage: MANAGED,
            files: RouteFiles::Bucket,
            prepare: integration,
            expected_plan: &[PULLS],
            expected_sent: &[
                OWN_STAGE,
                MANAGED_REF,
                "METADATA$FILE_ROW_NUMBER",
                "METADATA$FILENAME",
            ],
            ..BASE
        },
        RouteTestCase {
            description: "shaping = loader keeps them on SQLLoad, and plan says why",
            stage: MANAGED,
            settings: "write_strategy = \"append\"\nshaping = \"loader\"",
            files: RouteFiles::Bucket,
            prepare: integration,
            expected_plan: &[
                WRITES_PARQUET,
                "SQLLoad reads the files because the table sets shaping = \"loader\"",
            ],
            expected_unsent: &["METADATA$FILENAME"],
            ..BASE
        },
        RouteTestCase {
            description: "a hashed column on the direct route: a temporary table and the hashing insert in one request, so no plain values outlive the session",
            stage: MANAGED,
            settings: "write_strategy = \"append\"\n[sources.orders.tables.orders]\nmask = { extra = \"hash\" }",
            files: RouteFiles::Bucket,
            file_set: with_text,
            prepare: integration,
            expected_plan: &[PULLS, "in a temporary table, gone with its session"],
            expected_sent: &["CREATE OR REPLACE TEMPORARY TABLE", "SHA2(\"EXTRA\", 256)"],
            expected_unsent: &["TRANSIENT"],
            ..BASE
        },
        RouteTestCase {
            description: "a hashed column under replace, which spans commits, stays on SQLLoad",
            stage: MANAGED,
            settings: "write_strategy = \"replace\"\n[sources.orders.tables.orders]\nmask = { extra = \"hash\" }",
            files: RouteFiles::Bucket,
            file_set: with_text,
            prepare: integration,
            expected_plan: &[
                WRITES_PARQUET,
                "extra is hashed and the table is rewritten as a whole",
            ],
            expected_unsent: &["SHA2("],
            ..BASE
        },
        RouteTestCase {
            description: "JSON the files hold as text: the direct COPY parses it",
            stage: MANAGED,
            settings: "write_strategy = \"append\"\n[sources.orders.tables.orders]\ncolumns = { extra = \"json\" }",
            files: RouteFiles::Bucket,
            file_set: with_text,
            prepare: integration,
            expected_plan: &[PULLS],
            expected_sent: &["PARSE_JSON(CAST(GET($1, 'extra') AS VARCHAR))"],
            ..BASE
        },
        RouteTestCase {
            description: "and so does the staged route's COPY, with no staging table",
            settings: "write_strategy = \"append\"\n[sources.orders.tables.orders]\ncolumns = { extra = \"json\" }",
            file_set: with_text,
            expected_sent: &[
                "PARSE_JSON(CAST(GET($1, 'extra') AS VARCHAR))",
                "FROM @~/sqlload/",
            ],
            expected_unsent: &["TRANSIENT"],
            ..BASE
        },
        RouteTestCase {
            description: "per-file names passing the statement limit: several COPYs in one transaction",
            stage: MANAGED,
            files: RouteFiles::Bucket,
            file_set: long_names,
            prepare: integration,
            expected_plan: &[PULLS],
            expected_sent: &["BEGIN", "COMMIT", "COPY INTO"],
            ..BASE
        },
        RouteTestCase {
            description: "a later batch the direct checks refuse goes via SQLLoad; the batch before stays direct",
            stage: MANAGED,
            files: RouteFiles::Bucket,
            file_set: binary_later,
            prepare: integration,
            expected_plan: &[PULLS],
            expected_sent: &["METADATA$FILENAME", "/_sqlload/"],
            ..BASE
        },
    ];
    run_async(async {
        for test_case in &test_cases {
            let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
            let outcome = run_route(directory.path(), test_case).await?;
            let place = test_case.description;
            assert_eq!(
                outcome.error.contains(test_case.expected_error),
                true,
                "{place}: {}",
                outcome.error
            );
            assert_eq!(
                outcome.error.is_empty(),
                test_case.expected_error.is_empty(),
                "{place}: {}",
                outcome.error
            );
            assert_eq!(
                not_found(std::slice::from_ref(&outcome.plan), test_case.expected_plan),
                Vec::<&str>::new(),
                "{place}: {}",
                outcome.plan
            );
            assert_eq!(
                not_found(&outcome.sent, test_case.expected_sent),
                Vec::<&str>::new(),
                "{place}: {:#?}",
                outcome.sent
            );
            assert_eq!(
                found(&outcome.sent, test_case.expected_unsent),
                Vec::<&str>::new(),
                "{place}: {:#?}",
                outcome.sent
            );
            assert_eq!(
                test_case.expected_fetches.unwrap_or(outcome.fetches),
                outcome.fetches,
                "{place}"
            );
            assert_eq!(
                outcome.staged_left, test_case.expected_staged_left,
                "{place}"
            );
            assert!(outcome.sequence_ids_rise, "{place}");
        }
        Ok(())
    })
}
