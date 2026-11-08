use crate::helpers::{
    every_csv_type, every_ndjson_type, every_parquet_storage, every_parquet_type, flaky_probes,
    found, half_float_later, int96_in_a_later_batch, int96_newest, integration,
    integration_and_orders, json_layouts, long_names, long_names_with_text, nanos_in_a_later_batch,
    nanos_newest, no_stage_or_channel, no_stage_or_host, no_user_stage, not_found, not_owned,
    nothing, old_leftovers, run_async, run_debug, run_route, similar_integration, stage_exists,
    stage_on_other_integration, two_batches, two_days, with_text,
};
use crate::test_types::{
    DebugStageTestCase, MaskMatrixTestCase, RouteFileSet, RouteFiles, RouteTestCase,
};

const MANAGED: &str = "{ type = \"external_stage\", url = \"s3://fake-bucket/\", storage_integration = \"LAKE_INT\", region = \"us-east-1\", endpoint = \"FAKE\" }";
const KEEPS: &str = "{ type = \"external_stage\", url = \"s3://fake-bucket/\", storage_integration = \"LAKE_INT\", region = \"us-east-1\", endpoint = \"FAKE\", keep_staged_files = true }";
const OWN_STAGE: &str = "COPY INTO \"DB\".\"RAW\".\"ORDERS\"";
const MANAGED_REF: &str = "FROM @\"DB\".\"RAW\".\"_SQLLOAD_STAGE\"";
const WRITES_PARQUET: &str = "Parquet -> ";
const FILES_STRATEGIES: [&str; 3] = [
    "write_strategy = \"append\"\n",
    "write_strategy = \"delete_insert\"\ncursor_column = \"day\"\n",
    "write_strategy = \"replace\"\n",
];
const FORMATS: [(&str, &str, (RouteFileSet, &str)); 5] = [
    ("parquet, every type", "", (every_parquet_type, "note")),
    (
        "parquet, every storage",
        "",
        (every_parquet_storage, "names"),
    ),
    ("ndjson", "format = \"ndjson\"\n", (every_ndjson_type, "s")),
    ("json", "format = \"json\"\n", (json_layouts, "v")),
    ("csv", "format = \"csv\"\n", (every_csv_type, "s")),
];
const PULLS_ANY: &str = "COPYs directly from s3://fake-bucket/src/";
const READS: &str = " into Arrow";
const PULLS: &str =
    "Snowflake COPYs directly from s3://fake-bucket/src/ (nothing through the loader)";

const USER: &str = "{ type = \"internal_user_stage\" }";
const TABLE: &str = "{ type = \"internal_table_stage\" }";
const STREAMING: &str = "{ type = \"streaming\" }";
const VIA_LOADER: &str = "write_strategy = \"append\"\ntest_loader_route = true";
const DIRECT: &str = "write_strategy = \"append\"";
const REPLACING: &str =
    "write_strategy = \"delete_insert\"\ncursor_column = \"day\"\ntest_loader_route = true";

const BASE: RouteTestCase = RouteTestCase {
    description: "",
    stage: USER,
    settings: VIA_LOADER,
    files: RouteFiles::Directory,
    file_set: two_days,
    prepare: nothing,
    reload: false,
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
            expected_plan: &["read parquet into Arrow -> Parquet -> PUT to @~ -> COPY"],
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
            description: "files no external stage covers fail the plan with the fix, never going through SQLLoad",
            settings: DIRECT,
            expected_error: "cannot be read by Snowflake: no external stage covers it\n    └── fix     add a storage integration that allows this bucket (the fast path)",
            expected_unsent: &["COPY INTO"],
            ..BASE
        },
        RouteTestCase {
            description: "copy_to_internal_stage: the files are uploaded unchanged to @~ and Snowflake loads them there, matching them by their @~/ names, then they are removed",
            settings: "write_strategy = \"append\"\ncopy_to_internal_stage = true",
            expected_plan: &[
                "src/ uploaded unchanged to @~ -> COPY",
                "copy_to_internal_stage: every file is downloaded and uploaded to @~ (much slower than a direct load).",
            ],
            expected_sent: &[
                OWN_STAGE,
                "FROM @~/sqlload/",
                "METADATA$FILENAME = '@~/sqlload/",
                "REMOVE @~/sqlload/",
            ],
            expected_unplanned: &[WRITES_PARQUET],
            ..BASE
        },
        RouteTestCase {
            description: "copy_to_internal_stage refuses NDJSON at plan, whose later keys it would miss; nothing is uploaded",
            settings: "write_strategy = \"append\"\nformat = \"ndjson\"\ncopy_to_internal_stage = true",
            file_set: every_ndjson_type,
            expected_error: "copy_to_internal_stage cannot load JSON or NDJSON yet",
            expected_unsent: &["COPY INTO", "/00001/"],
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
            expected_plan: &["Parquet -> PUT to @%table -> COPY"],
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
            description: "a stage on another integration fails the plan with the reason and the fix, not as a check that could not run",
            stage: MANAGED,
            prepare: stage_on_other_integration,
            expected_error: "cannot be used: stage _SQLLOAD_STAGE uses storage integration OTHER_INT, but the connection names LAKE_INT\n    └── fix     drop the stage so SQLLoad recreates it, or correct storage_integration",
            expected_unsent: &["COPY INTO"],
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
            settings: DIRECT,
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
            description: "path_columns captures and constant_columns load directly, as each file's literals",
            stage: MANAGED,
            settings: "write_strategy = \"append\"\npath_columns = \"day=*/part-{part:int}.parquet\"\nconstant_columns = { shop = \"main\", as_of = 2026-11-08 }",
            files: RouteFiles::Bucket,
            prepare: integration,
            expected_plan: &[PULLS],
            expected_sent: &[
                "METADATA$FILENAME = 'src/day=2026-11-01/part-00001.parquet' THEN CAST('1' AS NUMBER(38,0))",
                "THEN CAST('main' AS VARCHAR)",
                "THEN CAST('2026-11-08' AS DATE)",
            ],
            expected_unsent: &["/_sqlload/"],
            ..BASE
        },
        RouteTestCase {
            description: "a column the newest files hold as nanoseconds loads directly, kept whole",
            stage: MANAGED,
            settings: DIRECT,
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
            settings: DIRECT,
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
            description: "an INT96 under an embedded microsecond type fails the plan, naming the column and reason, and nothing loads",
            stage: MANAGED,
            settings: DIRECT,
            files: RouteFiles::Bucket,
            file_set: int96_newest,
            prepare: integration,
            expected_error: "cannot load these files exactly by reading them itself",
            expected_plan: &["at is Timestamp(", "stored as INT96"],
            expected_unsent: &["COPY INTO", "/_sqlload/"],
            ..BASE
        },
        RouteTestCase {
            description: "a later batch of INT96 under embedded microseconds fails the load after the clean batch before it, never going through SQLLoad",
            stage: MANAGED,
            settings: DIRECT,
            files: RouteFiles::Bucket,
            file_set: int96_in_a_later_batch,
            prepare: integration,
            expected_error: "never go through SQLLoad instead",
            expected_plan: &[PULLS],
            expected_sent: &["METADATA$FILENAME = 'src/day=2026-11-01/"],
            expected_unsent: &["/_sqlload/", "METADATA$FILENAME = 'src/day=2026-11-03/"],
            ..BASE
        },
        RouteTestCase {
            description: "a stage goes in the target's loader schema, whatever schema the table loads into, and apply creates only that one for it",
            stage: MANAGED,
            settings: "schema = \"OTHER\"\nwrite_strategy = \"append\"\ntest_loader_route = true",
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
            description: "a hashed batch past one request's size is split over several requests, each hashing its COPYs into a delta table of hashed values that one last request writes from",
            stage: MANAGED,
            settings: "write_strategy = \"append\"\n[sources.orders.tables.orders]\nmask = { extra = \"hash\" }",
            files: RouteFiles::Bucket,
            file_set: long_names_with_text,
            prepare: integration,
            expected_sent: &[
                "CREATE OR REPLACE TEMPORARY TABLE",
                "SHA2(\"EXTRA\", 256)",
                "_DELTA\"",
                "INSERT INTO \"DB\".\"RAW\".\"ORDERS\" (",
                "DROP TABLE IF EXISTS \"DB\".\"RAW\".\"ORDERS__SQLLOAD_STAGING_",
            ],
            expected_unsent: &["/_sqlload/"],
            ..BASE
        },
        RouteTestCase {
            description: "a direct append past one request's size is split the same way, COPYing into a delta table that one last request writes from",
            stage: MANAGED,
            settings: DIRECT,
            files: RouteFiles::Bucket,
            file_set: long_names_with_text,
            prepare: integration,
            expected_sent: &[
                "CREATE OR REPLACE TEMPORARY TABLE",
                "_DELTA\"",
                "INSERT INTO \"DB\".\"RAW\".\"ORDERS\" (",
            ],
            expected_unsent: &["/_sqlload/", "SHA2("],
            ..BASE
        },
        RouteTestCase {
            description: "hmac masking is the one case files Snowflake reads itself go through SQLLoad, warned in plan",
            stage: MANAGED,
            settings: "write_strategy = \"append\"\nmask_key = \"k\"\n[sources.orders.tables.orders]\nmask = { extra = \"hmac\" }",
            files: RouteFiles::Bucket,
            file_set: with_text,
            prepare: integration,
            expected_plan: &[
                WRITES_PARQUET,
                "SQLLoad reads the files because extra masked with hmac, whose key must never appear in SQL",
                "extra: hmac masking pulls every file through the loader",
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
            description: "replace on the direct route: each batch COPYs straight into the build table that is swapped in, rows written once",
            stage: MANAGED,
            settings: "write_strategy = \"replace\"",
            files: RouteFiles::Bucket,
            file_set: two_batches,
            prepare: integration,
            expected_plan: &[PULLS],
            expected_sent: &[
                "COPY INTO \"DB\".\"RAW\".\"ORDERS__SQLLOAD_STAGING_",
                "METADATA$FILENAME",
            ],
            expected_unsent: &["INSERT INTO", "/_sqlload/", "TEMPORARY TABLE"],
            ..BASE
        },
        RouteTestCase {
            description: "--reload on the direct route: each batch COPYs straight into the build table, which is swapped in once",
            stage: MANAGED,
            settings: "write_strategy = \"append\"",
            files: RouteFiles::Bucket,
            file_set: two_batches,
            prepare: integration_and_orders,
            reload: true,
            expected_plan: &[PULLS, "recreate from the whole source (--reload)"],
            expected_sent: &[
                "COPY INTO \"DB\".\"RAW\".\"ORDERS__SQLLOAD_STAGING_",
                "SWAP WITH",
                "METADATA$FILENAME",
            ],
            expected_unsent: &["INSERT INTO", "/_sqlload/", "TEMPORARY TABLE"],
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
            settings: "write_strategy = \"append\"\ntest_loader_route = true\n[sources.orders.tables.orders]\ncolumns = { extra = \"json\" }",
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
            settings: DIRECT,
            files: RouteFiles::Bucket,
            file_set: long_names,
            prepare: integration,
            expected_plan: &[PULLS],
            expected_sent: &["BEGIN", "COMMIT", "COPY INTO"],
            ..BASE
        },
        RouteTestCase {
            description: "a later batch the direct checks refuse fails the load; the batch before loaded directly and nothing goes through SQLLoad",
            stage: MANAGED,
            settings: DIRECT,
            files: RouteFiles::Bucket,
            file_set: half_float_later,
            prepare: integration,
            expected_error: "never go through SQLLoad instead: ",
            expected_plan: &[PULLS],
            expected_sent: &["METADATA$FILENAME"],
            expected_unsent: &["/_sqlload/"],
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

/// Every format and type the fixtures hold, under every strategy a files source takes (merge is refused for files sources), each `FORMATS` entry naming a text column to mask, unmasked, hashed and hmac-masked: only hmac reaches SQLLoad's reader.
#[test]
fn given_files_a_warehouse_reads_itself_when_planning_every_format_type_and_strategy_then_only_hmac_reaches_sqlloads_reader()
-> Result<(), String> {
    let test_cases = [
        MaskMatrixTestCase {
            description: "unmasked",
            mask: "",
            expected_plan: &[PULLS_ANY],
            expected_unplanned: &[READS],
        },
        MaskMatrixTestCase {
            description: "hash",
            mask: "[sources.orders.tables.orders]\nmask = { COLUMN = \"hash\" }",
            expected_plan: &[PULLS_ANY],
            expected_unplanned: &[READS],
        },
        MaskMatrixTestCase {
            description: "hmac",
            mask: "mask_key = \"k\"\n[sources.orders.tables.orders]\nmask = { COLUMN = \"hmac\" }",
            expected_plan: &[READS],
            expected_unplanned: &[PULLS_ANY],
        },
    ];
    run_async(async {
        for test_case in &test_cases {
            for ((format_name, format, (file_set, column)), strategy) in
                FORMATS.iter().flat_map(|format| {
                    FILES_STRATEGIES
                        .iter()
                        .map(move |strategy| (format, strategy))
                })
            {
                let mask = test_case.mask.replace("COLUMN", column);
                let route_case = RouteTestCase {
                    description: format!(
                        "{}, {format_name}: {strategy}{mask}",
                        test_case.description
                    )
                    .leak(),
                    stage: MANAGED,
                    settings: format!("{format}{strategy}{mask}").leak(),
                    files: RouteFiles::Bucket,
                    file_set: *file_set,
                    prepare: integration,
                    ..BASE
                };
                let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
                let outcome = run_route(directory.path(), &route_case).await?;
                assert_eq!(
                    not_found(std::slice::from_ref(&outcome.plan), test_case.expected_plan),
                    Vec::<&str>::new(),
                    "{}: {} {}",
                    route_case.description,
                    outcome.plan,
                    outcome.error
                );
                assert_eq!(
                    found(
                        std::slice::from_ref(&outcome.plan),
                        test_case.expected_unplanned
                    ),
                    Vec::<&str>::new(),
                    "{}: {}",
                    route_case.description,
                    outcome.plan
                );
            }
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
