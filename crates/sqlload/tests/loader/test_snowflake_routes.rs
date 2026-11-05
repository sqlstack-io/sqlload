use crate::helpers::{
    flaky_probes, found, half_float_later, int96_in_a_later_batch, int96_newest, integration,
    long_names, long_names_with_text, nanos_in_a_later_batch, nanos_newest, no_stage_or_channel,
    no_stage_or_host, no_user_stage, not_found, not_owned, nothing, old_leftovers, run_async,
    run_debug, run_route, similar_integration, stage_exists, two_batches, two_days, with_text,
};
use crate::test_types::{DebugStageTestCase, RouteFiles, RouteTestCase};

const MANAGED: &str = "{ type = \"external_stage\", url = \"s3://fake-bucket/\", storage_integration = \"LAKE_INT\", region = \"us-east-1\", endpoint = \"FAKE\" }";
const KEEPS: &str = "{ type = \"external_stage\", url = \"s3://fake-bucket/\", storage_integration = \"LAKE_INT\", region = \"us-east-1\", endpoint = \"FAKE\", keep_staged_files = true }";
const OWN_STAGE: &str = "COPY INTO \"DB\".\"RAW\".\"ORDERS\"";
const MANAGED_REF: &str = "FROM @\"DB\".\"RAW\".\"_SQLLOAD_STAGE\"";
const WRITES_PARQUET: &str = "write Parquet to";
const PULLS: &str = "destination loads parquet files from s3://fake-bucket/src/";

const USER: &str = "{ type = \"internal_user_stage\" }";
const TABLE: &str = "{ type = \"internal_table_stage\" }";
const STREAMING: &str = "{ type = \"streaming\" }";
const APPEND: &str = "write_strategy = \"append\"";
const REPLACING: &str = "write_strategy = \"delete_insert\"\ncursor_column = \"day\"";

const BASE: RouteTestCase = RouteTestCase {
    description: "",
    stage: USER,
    settings: APPEND,
    files: RouteFiles::Directory,
    file_set: two_days,
    prepare: nothing,
    expected_error: "",
    expected_plan: &[],
    expected_unplanned: &[],
    expected_sent: &[],
    expected_unsent: &[],
    expected_fetches: None,
    expected_staged_left: 0,
};

#[test]
fn given_a_snowflake_target_when_planning_and_loading_then_rows_take_the_route_its_load_method_sets()
-> Result<(), String> {
    let test_cases = [
        RouteTestCase {
            description: "@~: one credentials fetch for the plan's probe and one for each of the two batches",
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
            description: "@~ refused: the plan fails and never falls back",
            prepare: no_user_stage,
            expected_error: "load_method @~ cannot be used",
            expected_unsent: &["COPY INTO"],
            ..BASE
        },
        RouteTestCase {
            description: "@%table: an append into a table this load creates uses its own table stage",
            stage: TABLE,
            expected_plan: &["write Parquet to internal stage @%table"],
            expected_sent: &[
                OWN_STAGE,
                "FROM @\"DB\".\"RAW\".%\"ORDERS\"/sqlload/",
                "REMOVE @\"DB\".\"RAW\".%\"ORDERS\"/sqlload/",
            ],
            ..BASE
        },
        RouteTestCase {
            description: "@%table: a delete_insert uses the staging table's stage, after probing with a temporary table",
            stage: TABLE,
            settings: REPLACING,
            expected_plan: &["the staging table's @%table"],
            expected_sent: &[
                "CREATE TEMPORARY TABLE IF NOT EXISTS",
                ".%\"ORDERS__SQLLOAD_STAGING_",
                "DELETE FROM \"DB\".\"RAW\".\"ORDERS\"",
            ],
            ..BASE
        },
        RouteTestCase {
            description: "@%table: an append into a table this role does not own goes through the staging table's stage",
            stage: TABLE,
            prepare: not_owned,
            expected_plan: &["the staging table's @%table"],
            expected_sent: &[
                ".%\"ORDERS__SQLLOAD_STAGING_",
                "INSERT INTO \"DB\".\"RAW\".\"ORDERS\"",
            ],
            ..BASE
        },
        RouteTestCase {
            description: "streaming: Snowpipe Streaming into a staging table, then the usual write",
            stage: STREAMING,
            settings: REPLACING,
            expected_plan: &["Snowpipe Streaming into a transient staging table"],
            expected_sent: &[
                "CREATE OR REPLACE TRANSIENT TABLE",
                "DELETE FROM \"DB\".\"RAW\".\"ORDERS\"",
            ],
            expected_unsent: &["COPY INTO", "FROM VALUES"],
            ..BASE
        },
        RouteTestCase {
            description: "an append streaming goes through staging too, with rows keyed by stored names and values as text",
            stage: STREAMING,
            expected_plan: &["Snowpipe Streaming into a transient staging table"],
            expected_sent: &[
                "INSERT INTO \"DB\".\"RAW\".\"ORDERS\"",
                "\"ID\":\"1\"",
                "\"_SQLLOAD_SOURCE_ROW\":\"1\"",
            ],
            expected_unsent: &["COPY INTO"],
            ..BASE
        },
        RouteTestCase {
            description: "streaming: its host unreachable fails the plan",
            stage: STREAMING,
            prepare: no_stage_or_host,
            expected_error: "load_method Snowpipe Streaming cannot be used",
            ..BASE
        },
        RouteTestCase {
            description: "streaming: a channel that will not open fails the load, with no INSERT",
            stage: STREAMING,
            prepare: no_stage_or_channel,
            expected_error: "Snowpipe Streaming will not open",
            expected_unsent: &["FROM VALUES"],
            ..BASE
        },
        RouteTestCase {
            description: "insert goes straight to batched INSERT into staging, warned in plan, with no probes",
            stage: "{ type = \"insert\" }",
            expected_plan: &[
                "batched INSERT into a transient staging table",
                "rows load by batched INSERT",
            ],
            expected_sent: &["FROM VALUES", "INSERT INTO \"DB\".\"RAW\".\"ORDERS\""],
            expected_unsent: &["COPY INTO"],
            expected_fetches: Some(0),
            ..BASE
        },
        RouteTestCase {
            description: "a named internal stage",
            stage: "{ type = \"internal_named_stage\", name = \"DB.RAW.LOADS\" }",
            expected_plan: &["internal stage @DB.RAW.LOADS"],
            expected_sent: &[
                "FROM @DB.RAW.LOADS/sqlload/",
                "REMOVE @DB.RAW.LOADS/sqlload/",
            ],
            ..BASE
        },
        RouteTestCase {
            description: "a named internal stage refused fails the plan",
            stage: "{ type = \"internal_named_stage\", name = \"DB.RAW.LOADS\" }",
            prepare: |fake| fake.state.lock().denied_stages.push("LOADS".to_owned()),
            expected_error: "load_method @DB.RAW.LOADS cannot be used",
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
            description: "a column the newest files hold as nanoseconds loads directly, kept whole",
            stage: MANAGED,
            files: RouteFiles::Bucket,
            file_set: nanos_newest,
            prepare: integration,
            expected_plan: &[PULLS],
            expected_unplanned: &["does not load exactly"],
            expected_sent: &["METADATA$FILENAME", "TIMESTAMP_NTZ(9)"],
            ..BASE
        },
        RouteTestCase {
            description: "a later batch of nanoseconds loads directly like the microsecond batches around it",
            stage: MANAGED,
            files: RouteFiles::Bucket,
            file_set: nanos_in_a_later_batch,
            prepare: integration,
            expected_plan: &[PULLS],
            expected_sent: &[
                "METADATA$FILENAME = 'src/day=2026-11-01/",
                "METADATA$FILENAME = 'src/day=2026-11-02/",
                "METADATA$FILENAME = 'src/day=2026-11-03/",
            ],
            expected_unsent: &["/_sqlload/"],
            ..BASE
        },
        RouteTestCase {
            description: "an INT96 under an embedded microsecond type stays on SQLLoad, and plan names the column and reason",
            stage: MANAGED,
            files: RouteFiles::Bucket,
            file_set: int96_newest,
            prepare: integration,
            expected_plan: &[WRITES_PARQUET, "at is Timestamp(", "stored as INT96"],
            expected_unsent: &["METADATA$FILENAME"],
            ..BASE
        },
        RouteTestCase {
            description: "a later batch of INT96 under embedded microseconds goes via SQLLoad, and the clean batches around it stay direct",
            stage: MANAGED,
            files: RouteFiles::Bucket,
            file_set: int96_in_a_later_batch,
            prepare: integration,
            expected_plan: &[PULLS],
            expected_sent: &[
                "METADATA$FILENAME = 'src/day=2026-11-01/",
                "METADATA$FILENAME = 'src/day=2026-11-03/",
                "/_sqlload/",
            ],
            ..BASE
        },
        RouteTestCase {
            description: "a stage goes in the target's loader schema, whatever schema the table loads into, and apply creates only that one for it",
            stage: MANAGED,
            settings: "schema = \"OTHER\"\nwrite_strategy = \"append\"",
            prepare: integration,
            expected_plan: &["\"DB\".\"RAW\".\"_SQLLOAD_STAGE\""],
            expected_sent: &[
                "CREATE SCHEMA IF NOT EXISTS \"DB\".\"RAW\"",
                "CREATE STAGE IF NOT EXISTS \"DB\".\"RAW\".\"_SQLLOAD_STAGE\"",
                "COPY INTO \"DB\".\"OTHER\".\"ORDERS\"",
            ],
            expected_unsent: &["\"OTHER\".\"_SQLLOAD_STAGE\""],
            ..BASE
        },
        RouteTestCase {
            description: "plan lists the stage's own folder, never its whole URL",
            stage: MANAGED,
            prepare: stage_exists,
            expected_sent: &["LIST @\"DB\".\"RAW\".\"_SQLLOAD_STAGE\"/_sqlload/"],
            ..BASE
        },
        RouteTestCase {
            description: "an integration only matching through SHOW's wildcard does not count",
            stage: MANAGED,
            prepare: similar_integration,
            expected_error: "storage integration LAKE_INT does not exist",
            ..BASE
        },
        RouteTestCase {
            description: "a probe failing for any other reason fails the plan, never changing the route",
            prepare: flaky_probes,
            expected_error: "cannot check whether load_method @~ can be used",
            expected_unsent: &["COPY INTO", "FROM VALUES"],
            ..BASE
        },
        RouteTestCase {
            description: "a hashed batch past 900 KiB in total still goes direct, each statement being within the limit",
            stage: MANAGED,
            settings: "write_strategy = \"append\"\n[sources.orders.tables.orders]\nmask = { extra = \"hash\" }",
            files: RouteFiles::Bucket,
            file_set: long_names_with_text,
            prepare: integration,
            expected_sent: &["CREATE OR REPLACE TEMPORARY TABLE", "SHA2(\"EXTRA\", 256)"],
            expected_unsent: &["/_sqlload/"],
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
            description: "a hashed column under replace: each batch's temporary table is hashed into the table that is swapped in",
            stage: MANAGED,
            settings: "write_strategy = \"replace\"\n[sources.orders.tables.orders]\nmask = { extra = \"hash\" }",
            files: RouteFiles::Bucket,
            file_set: with_text,
            prepare: integration,
            expected_plan: &[PULLS, "in a temporary table, gone with its session"],
            expected_sent: &[
                "CREATE OR REPLACE TEMPORARY TABLE",
                "_HASHED\" (",
                "SHA2(\"EXTRA\", 256)",
                "SWAP WITH",
            ],
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
            file_set: half_float_later,
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
            assert!(
                outcome.error.contains(test_case.expected_error),
                "{}: {}",
                test_case.description,
                outcome.error
            );
            assert_eq!(
                outcome.error.is_empty(),
                test_case.expected_error.is_empty(),
                "{}: {}",
                test_case.description,
                outcome.error
            );
            assert_eq!(
                not_found(std::slice::from_ref(&outcome.plan), test_case.expected_plan),
                Vec::<&str>::new(),
                "{}: {}",
                test_case.description,
                outcome.plan
            );
            assert_eq!(
                found(
                    std::slice::from_ref(&outcome.plan),
                    test_case.expected_unplanned
                ),
                Vec::<&str>::new(),
                "{}: {}",
                test_case.description,
                outcome.plan
            );
            assert_eq!(
                not_found(&outcome.sent, test_case.expected_sent),
                Vec::<&str>::new(),
                "{}: {:#?}",
                test_case.description,
                outcome.sent
            );
            assert_eq!(
                found(&outcome.sent, test_case.expected_unsent),
                Vec::<&str>::new(),
                "{}: {:#?}",
                test_case.description,
                outcome.sent
            );
            assert_eq!(
                test_case.expected_fetches.unwrap_or(outcome.fetches),
                outcome.fetches,
                "{}",
                test_case.description
            );
            assert_eq!(
                outcome.staged_left, test_case.expected_staged_left,
                "{}",
                test_case.description
            );
            assert!(outcome.sequence_ids_rise, "{}", test_case.description);
        }
        Ok(())
    })
}

#[test]
fn given_an_external_stage_when_debugging_then_its_state_is_reported() -> Result<(), String> {
    let test_cases = [
        DebugStageTestCase {
            description: "a stage that exists and lists",
            prepare: stage_exists,
            expected_line: "ok    target dev  LOADER; stage @\"DB\".\"RAW\".\"_SQLLOAD_STAGE\" reads s3://fake-bucket/ and lists",
            expected_sent: "LIST @\"DB\".\"RAW\".\"_SQLLOAD_STAGE\"/_sqlload/",
        },
        DebugStageTestCase {
            description: "a stage apply will create",
            prepare: integration,
            expected_line: "is missing; apply creates it",
            expected_sent: "SHOW STAGES",
        },
        DebugStageTestCase {
            description: "a schema that does not exist yet is tolerated, as plan does",
            prepare: |fake| fake.state.lock().schema_missing = true,
            expected_line: "is missing; apply creates it",
            expected_sent: "SHOW STAGES",
        },
    ];
    run_async(async {
        for test_case in &test_cases {
            let (lines, sent) = run_debug(MANAGED, test_case).await?;
            assert_eq!(
                not_found(&lines, &[test_case.expected_line]),
                Vec::<&str>::new(),
                "{}: {lines:#?}",
                test_case.description
            );
            assert_eq!(
                not_found(&sent, &[test_case.expected_sent]),
                Vec::<&str>::new(),
                "{}: {sent:#?}",
                test_case.description
            );
        }
        Ok(())
    })
}
