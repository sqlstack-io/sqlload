use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres_version::start_postgres_version;
use sqlload_testkit::seaweedfs::main::create_bucket::create_bucket;
use sqlload_testkit::seaweedfs::main::start_seaweedfs::start_seaweedfs;

use crate::helpers::{
    cdc_files_project, crashed_files_batch, delete_items_folder, download_out, duckdb_text,
    local_store, nothing_planted, plant_unfinished_commit, replace_items_with_unfinished_copy,
    run_async, run_cdc_steps, seaweedfs_store, step,
};
use crate::test_postgres_cdc::{
    ADD_COLUMN, BACKFILL_CHANGELOG, CHANGELOG_RESULT, CHANGES, EXTRA_TABLE, LARGE_TRANSACTION, LOG,
    SETUP,
};
use crate::test_types::{CdcFilesTestCase, CdcStep};

const FILES_CHANGELOG_QUERY: &str = "SELECT (SELECT string_agg(_sqlload_seq || ':' || _sqlload_op || ':' || id, ',' ORDER BY _sqlload_lsn, _sqlload_seq, id) FROM read_parquet('{root}/out/log/orders/*.parquet', union_by_name = true)) || ' | ' || (SELECT string_agg(_sqlload_op || ':' || id, ',' ORDER BY _sqlload_lsn, _sqlload_seq, id) FROM read_parquet('{root}/out/log/items/*.parquet', union_by_name = true)) || ' | ' || (SELECT string_agg(_sqlload_unchanged, ';') FROM read_parquet('{root}/out/log/orders/*.parquet', union_by_name = true) WHERE _sqlload_op = 'update')";

const FILES_PLANTED_QUERY: &str = "SELECT (SELECT string_agg(_sqlload_seq || ':' || _sqlload_op || ':' || id, ',' ORDER BY _sqlload_lsn, _sqlload_seq, id) FROM read_parquet('{root}/out/log/orders/*.parquet', union_by_name = true)) || ' | ' || (SELECT count(*) FROM glob('{root}/out/log/orders/**/*planted*'))";

const FILES_CAUGHT_UP: [CdcStep; 4] = [
    step("", LOG, "", (true, "loaded")),
    step(CHANGES, LOG, "", (true, "Completed")),
    step("", LOG, "", (true, "0 rows")),
    step(
        "",
        &["plan", "log"],
        "",
        (true, "append changes (changelog)"),
    ),
];

const FILES_BEFORE_COMMIT: [CdcStep; 5] = crashed_files_batch("batch-before-commit");
const FILES_AFTER_FIRST: [CdcStep; 5] = crashed_files_batch("batch-after-first-table");
const FILES_BEFORE_CONFIRM: [CdcStep; 5] = crashed_files_batch("batch-before-confirm");

const FILES_PARTITIONED: &str = r#"
[sources.part]
kind = "postgres_cdc"
connection = "pg"
include = ["public.items"]
cdc_mode = "changelog"
destination_path = "parts/{table}/{cursor:date}/"

[sources.part.tables."public.items"]
description = "Items, captured."
"#;

const FILES_MERGED: &str = r#"
[sources.merged]
kind = "postgres_cdc"
connection = "pg"
include = ["public.items"]

[sources.down]
kind = "postgres"
connection = "pg"
include = ["public.["]
"#;

const MERGE_REFUSED: (bool, &str) = (
    false,
    "a files target takes kind = \"postgres_cdc\" only with cdc_mode = \"changelog\"",
);

const FILES_MERGE_REFUSED: [CdcStep; 7] = [
    step("", &["plan", "merged"], "", MERGE_REFUSED),
    step("", &["plan", "merged__items"], "", MERGE_REFUSED),
    step("", &["plan", "public.items"], "", MERGE_REFUSED),
    step("", &["plan"], "", MERGE_REFUSED),
    step(
        "",
        &["plan", "down"],
        "",
        (false, "invalid table pattern \"public.[\""),
    ),
    step(
        "",
        &["plan", "--exclude", "merged__items", "down"],
        "",
        (true, "Plan complete"),
    ),
    step("", LOG, "", (true, "loaded")),
];

const FILES_REFUSED: [CdcStep; 3] = [
    step("", LOG, "", (true, "loaded")),
    step(
        "",
        &["load", "log", "--reload", "--yes"],
        "",
        (
            false,
            "a postgres_cdc table is copied again with --resnapshot, not --reload",
        ),
    ),
    step(
        "",
        &["plan", "part"],
        "",
        (
            false,
            "part__items\n    └── reason  a postgres_cdc table writes one changelog, not partition folders",
        ),
    ),
];

const LOG_RESNAPSHOT: &[&str] = &["load", "log", "--resnapshot", "--yes"];

const FILES_FOLDER_DELETED: [CdcStep; 3] = [
    step("", LOG, "", (true, "loaded")),
    step(CHANGES, LOG, "", (true, "Completed")),
    step("", LOG, "", (true, "0 rows")),
];

const FILES_TYPE_CHANGED: [CdcStep; 4] = [
    step("", LOG, "", (true, "loaded")),
    step(
        "ALTER TABLE public.items ALTER COLUMN w TYPE text",
        LOG,
        "",
        (false, "--resnapshot for a postgres_cdc table"),
    ),
    step("", LOG_RESNAPSHOT, "", (true, "loaded")),
    step("", LOG, "", (true, "0 rows")),
];

const FILES_SNAPSHOT_CRASH: [CdcStep; 7] = [
    step("", LOG, "snapshot-after-first-table", (false, "")),
    step("", LOG, "", (true, "Completed")),
    step("", LOG_RESNAPSHOT, "", (true, "loaded")),
    step(CHANGES, LOG, "", (true, "Completed")),
    step(
        "SELECT pg_drop_replication_slot('sqlload_log')",
        LOG,
        "",
        (false, "slot sqlload_log is gone"),
    ),
    step("", LOG_RESNAPSHOT, "", (true, "loaded")),
    step("", LOG, "", (true, "0 rows")),
];

const FILES_COLUMN_ADDED: [CdcStep; 3] = [
    step("", LOG, "", (true, "loaded")),
    step(ADD_COLUMN, LOG, "", (true, "Completed")),
    step("", LOG, "", (true, "0 rows")),
];

const FILES_SPILLED: [CdcStep; 3] = [
    step("", LOG, "", (true, "loaded")),
    step(
        LARGE_TRANSACTION,
        LOG,
        "",
        (true, "spilling it to local disk"),
    ),
    step("", LOG, "", (true, "0 rows")),
];

const FILES_REJOIN: [CdcStep; 5] = [
    step("", LOG, "", (true, "loaded")),
    step(EXTRA_TABLE, LOG, "", (true, "Completed")),
    step(
        "DROP TABLE public.extra;",
        LOG,
        "",
        (false, "this load leaves out log__extra"),
    ),
    step("", LOG_RESNAPSHOT, "", (true, "loaded")),
    step(
        EXTRA_TABLE,
        LOG,
        "",
        (
            false,
            "are still in log__extra; copying those tables alone would replace them without asking, so run `sqlload load --resnapshot` to copy every table again (it asks first), or delete their folders",
        ),
    ),
];

/// A changelog into files keeps every change in exactly one row, whichever restart point a load stops at, an unfinished load's files never count as written, and what files cannot hold is refused at plan.
#[test]
fn given_a_postgres_cdc_changelog_into_files_when_loads_stop_at_each_restart_point_then_each_change_is_in_one_row()
-> Result<(), String> {
    let test_cases = [
        CdcFilesTestCase {
            description: "Postgres 15: snapshot, both transactions and a quiet re-run, each change once",
            tag: "15-alpine",
            steps: &FILES_CAUGHT_UP,
            store: local_store,
            spill_bytes: "",
            extra_config: "",
            plant: nothing_planted,
            check_query: FILES_CHANGELOG_QUERY,
            expected_result: CHANGELOG_RESULT,
        },
        CdcFilesTestCase {
            description: "an unfinished load's files, with a far later change, are deleted and move no floor",
            tag: "17-alpine",
            steps: &FILES_CAUGHT_UP,
            store: local_store,
            spill_bytes: "",
            extra_config: "",
            plant: plant_unfinished_commit,
            check_query: FILES_PLANTED_QUERY,
            expected_result: "0:snapshot:1,0:snapshot:2,0:snapshot:3,1:insert:4,2:update:1,1:delete:2 | 0",
        },
        CdcFilesTestCase {
            description: "stopped before the batch's first commit: the next load writes it once",
            tag: "17-alpine",
            steps: &FILES_BEFORE_COMMIT,
            store: local_store,
            spill_bytes: "",
            extra_config: "",
            plant: nothing_planted,
            check_query: FILES_CHANGELOG_QUERY,
            expected_result: CHANGELOG_RESULT,
        },
        CdcFilesTestCase {
            description: "stopped after the first table committed: its replay is skipped, the other table's written",
            tag: "17-alpine",
            steps: &FILES_AFTER_FIRST,
            store: local_store,
            spill_bytes: "",
            extra_config: "",
            plant: nothing_planted,
            check_query: FILES_CHANGELOG_QUERY,
            expected_result: CHANGELOG_RESULT,
        },
        CdcFilesTestCase {
            description: "stopped after every commit, before the slot confirmed: the replay writes nothing",
            tag: "17-alpine",
            steps: &FILES_BEFORE_CONFIRM,
            store: local_store,
            spill_bytes: "",
            extra_config: "",
            plant: nothing_planted,
            check_query: FILES_CHANGELOG_QUERY,
            expected_result: CHANGELOG_RESULT,
        },
        CdcFilesTestCase {
            description: "a source merging its changes, or one that cannot list its tables, is refused when selected, and a load of another source goes ahead",
            tag: "17-alpine",
            steps: &FILES_MERGE_REFUSED,
            store: local_store,
            spill_bytes: "",
            extra_config: FILES_MERGED,
            plant: nothing_planted,
            check_query: "SELECT CAST((SELECT count(*) FROM glob('{root}/out/merged/**')) AS VARCHAR) || ':' || CAST((SELECT count(*) > 0 FROM glob('{root}/out/log/orders/*.parquet')) AS VARCHAR)",
            expected_result: "0:true",
        },
        CdcFilesTestCase {
            description: "--reload and cursor partitions are refused",
            tag: "17-alpine",
            steps: &FILES_REFUSED,
            store: local_store,
            spill_bytes: "",
            extra_config: FILES_PARTITIONED,
            plant: nothing_planted,
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM read_parquet('{root}/out/log/orders/*.parquet')",
            expected_result: "3",
        },
        CdcFilesTestCase {
            description: "a stopped snapshot resumes, and --resnapshot replaces every older file",
            tag: "17-alpine",
            steps: &FILES_SNAPSHOT_CRASH,
            store: local_store,
            spill_bytes: "",
            extra_config: "",
            plant: nothing_planted,
            check_query: "SELECT string_agg(_sqlload_op || ':' || id, ',' ORDER BY id) FROM read_parquet('{root}/out/log/orders/*.parquet')",
            expected_result: "snapshot:1,snapshot:3,snapshot:4",
        },
        CdcFilesTestCase {
            description: "a recorded table whose folder was deleted is copied again alone, not just streamed into",
            tag: "17-alpine",
            steps: &FILES_FOLDER_DELETED,
            store: local_store,
            spill_bytes: "",
            extra_config: "",
            plant: delete_items_folder,
            check_query: "SELECT string_agg(_sqlload_op || ':' || id || ':' || name, ',' ORDER BY _sqlload_lsn, _sqlload_seq, id) FROM read_parquet('{root}/out/log/items/*.parquet')",
            expected_result: "snapshot:1:one!,snapshot:2:two,snapshot:3:three",
        },
        CdcFilesTestCase {
            description: "a recorded table whose folder holds only a copy stopped mid-write is copied again alone",
            tag: "17-alpine",
            steps: &FILES_FOLDER_DELETED,
            store: local_store,
            spill_bytes: "",
            extra_config: "",
            plant: replace_items_with_unfinished_copy,
            check_query: "SELECT string_agg(_sqlload_op || ':' || id || ':' || name, ',' ORDER BY _sqlload_lsn, _sqlload_seq, id) FROM read_parquet('{root}/out/log/items/*.parquet')",
            expected_result: "snapshot:1:one!,snapshot:2:two,snapshot:3:three",
        },
        CdcFilesTestCase {
            description: "a type change the files cannot hold blocks until --resnapshot rewrites every file",
            tag: "17-alpine",
            steps: &FILES_TYPE_CHANGED,
            store: local_store,
            spill_bytes: "",
            extra_config: "",
            plant: nothing_planted,
            check_query: "SELECT string_agg(id || ':' || typeof(w), ',' ORDER BY id) FROM read_parquet('{root}/out/log/items/*.parquet')",
            expected_result: "1:VARCHAR,2:VARCHAR",
        },
        CdcFilesTestCase {
            description: "a column added between loads is in the new files",
            tag: "17-alpine",
            steps: &FILES_COLUMN_ADDED,
            store: local_store,
            spill_bytes: "",
            extra_config: "",
            plant: nothing_planted,
            check_query: "SELECT string_agg(_sqlload_op || ':' || id || ':' || COALESCE(CAST(c AS VARCHAR), '-') || ':' || COALESCE(_sqlload_unchanged, '-'), ',' ORDER BY _sqlload_lsn, _sqlload_seq) FROM read_parquet('{root}/out/log/items/*.parquet', union_by_name = true) WHERE _sqlload_op <> 'snapshot'",
            expected_result: "update:1:-:,c,,insert:3:7:-,update:2:8:-",
        },
        CdcFilesTestCase {
            description: "a spilled transaction is one commit with each change once",
            tag: "17-alpine",
            steps: &FILES_SPILLED,
            store: local_store,
            spill_bytes: "100000",
            extra_config: "",
            plant: nothing_planted,
            check_query: "SELECT (SELECT count(DISTINCT (_sqlload_lsn, _sqlload_seq)) || '/' || count(*) || ':' || max(_sqlload_seq) FROM read_parquet('{root}/out/log/orders/*.parquet') WHERE _sqlload_op <> 'snapshot') || ' | ' || (SELECT count(DISTINCT regexp_extract(filename, 'part-(.*)-[0-9]+[.]parquet$', 1)) FROM read_parquet('{root}/out/log/orders/*.parquet', filename = true) WHERE _sqlload_op <> 'snapshot')",
            expected_result: "68017/68017:68017 | 1",
        },
        CdcFilesTestCase {
            description: "a table joining the source is copied alone, before the others' records",
            tag: "17-alpine",
            steps: &BACKFILL_CHANGELOG,
            store: local_store,
            spill_bytes: "",
            extra_config: "",
            plant: nothing_planted,
            check_query: "SELECT string_agg(_sqlload_op || ':' || id, ',' ORDER BY _sqlload_lsn, _sqlload_seq) FROM read_parquet('{root}/out/log/extra/*.parquet')",
            expected_result: "insert:9",
        },
        CdcFilesTestCase {
            description: "a table rejoining while its folder holds earlier files is refused",
            tag: "17-alpine",
            steps: &FILES_REJOIN,
            store: local_store,
            spill_bytes: "",
            extra_config: "",
            plant: nothing_planted,
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM read_parquet('{root}/out/log/orders/*.parquet')",
            expected_result: "3",
        },
        CdcFilesTestCase {
            description: "into object storage: stopped after the first table committed, each change once",
            tag: "17-alpine",
            steps: &FILES_AFTER_FIRST,
            store: seaweedfs_store,
            spill_bytes: "",
            extra_config: "",
            plant: nothing_planted,
            check_query: FILES_CHANGELOG_QUERY,
            expected_result: CHANGELOG_RESULT,
        },
    ];
    run_async(async {
        let server = start_seaweedfs().await?;
        create_bucket(&server, "example-bucket-test").await?;
        for test_case in test_cases {
            let postgres = start_postgres_version(test_case.tag).await?;
            let client = connect(&postgres).await?;
            client
                .batch_execute(SETUP)
                .await
                .map_err(|error| error.to_string())?;
            let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
            let root = folder.path().join("lake");
            let (store, connection) = (test_case.store)(&server.endpoint, &root);
            let config = folder.path().join("sqlload.toml");
            let project = cdc_files_project(postgres.port, &connection) + test_case.extra_config;
            std::fs::write(&config, project).map_err(|error| error.to_string())?;
            let (first, rest) = test_case.steps.split_at(1);
            run_cdc_steps(
                &client,
                &config,
                (first, 0, test_case.spill_bytes),
                test_case.description,
            )
            .await?;
            (test_case.plant)(&root)?;
            run_cdc_steps(
                &client,
                &config,
                (rest, 1, test_case.spill_bytes),
                test_case.description,
            )
            .await?;
            let copied = folder.path().join("copied");
            download_out(store, &copied).await?;
            let query = test_case
                .check_query
                .replace("{root}", &copied.display().to_string());
            assert_eq!(
                duckdb_text(&folder.path().join("check.duckdb"), &query)?,
                test_case.expected_result,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}
