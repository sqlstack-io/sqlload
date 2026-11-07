use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres_version::start_postgres_version;

use crate::helpers::{
    CDC_CHANGES, cdc_project, crashed_batch, duckdb_text, run_async, run_sqlload, step,
};
use crate::test_types::{CdcScenarioTestCase, CdcStep};

const SETUP: &str = "
CREATE TABLE public.orders (id int8 PRIMARY KEY, qty int4, big text, tags text[]);
CREATE TABLE public.items (id int8 PRIMARY KEY, name text);
INSERT INTO public.orders VALUES
  (1, 1, (SELECT string_agg(md5(g::text), '') FROM generate_series(1, 300) g), '{a,b}'),
  (2, 2, 'b', '{c}'), (3, 3, 'c', NULL);
INSERT INTO public.items VALUES (1, 'one'), (2, 'two');
";

const CHANGES: &str = CDC_CHANGES;

const MERGED_QUERY: &str = "SELECT (SELECT string_agg(id || ':' || qty || ':' || length(big) || ':' || COALESCE(tags, '-') || ':' || _sqlload_op || ':' || (_sqlload_deleted_at IS NOT NULL), ',' ORDER BY id) FROM raw.app__orders) || ' | ' || (SELECT string_agg(id || ':' || name || ':' || _sqlload_op, ',' ORDER BY id) FROM raw.app__items)";
const MERGED_RESULT: &str = "1:10:9600:{a,b}:update:false,2:2:1:{c}:delete:true,3:3:1:-:snapshot:false,4:4:1:{\"x y\",z}:insert:false | 1:one!:update,2:two:snapshot,3:three:insert";

const CHANGELOG_QUERY: &str = "SELECT (SELECT string_agg(_sqlload_seq || ':' || _sqlload_op || ':' || id, ',' ORDER BY _sqlload_lsn, _sqlload_seq, id) FROM raw.log__orders) || ' | ' || (SELECT string_agg(_sqlload_op || ':' || id, ',' ORDER BY _sqlload_lsn, _sqlload_seq, id) FROM raw.log__items)";
const CHANGELOG_RESULT: &str = "0:snapshot:1,0:snapshot:2,0:snapshot:3,1:insert:4,2:update:1,1:delete:2 | snapshot:1,snapshot:2,insert:3,update:1";

const APP: &[&str] = &["load", "app"];
const LOG: &[&str] = &["load", "log"];
const BOTH: &[&str] = &["load", "app", "log"];

const CAUGHT_UP: [CdcStep; 3] = [
    step("", BOTH, "", (true, "loaded")),
    step(CHANGES, BOTH, "", (true, "Completed")),
    step("", BOTH, "", (true, "0 rows")),
];

const APP_BEFORE_COMMIT: [CdcStep; 5] = crashed_batch(APP, "batch-before-commit");
const APP_AFTER_FIRST: [CdcStep; 5] = crashed_batch(APP, "batch-after-first-table");
const APP_BEFORE_CONFIRM: [CdcStep; 5] = crashed_batch(APP, "batch-before-confirm");
const LOG_BEFORE_COMMIT: [CdcStep; 5] = crashed_batch(LOG, "batch-before-commit");
const LOG_AFTER_FIRST: [CdcStep; 5] = crashed_batch(LOG, "batch-after-first-table");
const LOG_BEFORE_CONFIRM: [CdcStep; 5] = crashed_batch(LOG, "batch-before-confirm");

const SNAPSHOT_CRASH: [CdcStep; 7] = [
    step("", APP, "snapshot-after-first-table", (false, "")),
    step("", APP, "", (false, "no snapshot from slot sqlload_app")),
    step("", &["load", "app", "--resnapshot"], "", (true, "loaded")),
    step(CHANGES, APP, "", (true, "Completed")),
    step(
        "SELECT pg_drop_replication_slot('sqlload_app')",
        APP,
        "",
        (false, "slot sqlload_app is gone"),
    ),
    step("", &["load", "app", "--resnapshot"], "", (true, "loaded")),
    step("", APP, "", (true, "0 rows")),
];

/// Every cell of D296's restart table, each in a fresh server: the process stops itself at the point and a later load must leave each table exactly as the source has it, with no change lost or applied twice (a changelog would show a replayed change as a duplicate row).
#[test]
fn given_a_postgres_cdc_source_when_loads_stop_at_each_restart_point_then_the_tables_converge_without_duplicates()
-> Result<(), String> {
    let test_cases = [
        CdcScenarioTestCase {
            description: "Postgres 15: snapshot, both transactions, then a re-run of nothing",
            tag: "15-alpine",
            steps: &CAUGHT_UP,
            check_query: MERGED_QUERY,
            expected_result: MERGED_RESULT,
        },
        CdcScenarioTestCase {
            description: "Postgres 16: the changelog holds every change once",
            tag: "16-alpine",
            steps: &CAUGHT_UP,
            check_query: CHANGELOG_QUERY,
            expected_result: CHANGELOG_RESULT,
        },
        CdcScenarioTestCase {
            description: "Postgres 17: merge keeps the unchanged TOASTed value",
            tag: "17-alpine",
            steps: &CAUGHT_UP,
            check_query: MERGED_QUERY,
            expected_result: MERGED_RESULT,
        },
        CdcScenarioTestCase {
            description: "merge: stopped during a batch, before any table commits",
            tag: "17-alpine",
            steps: &APP_BEFORE_COMMIT,
            check_query: MERGED_QUERY,
            expected_result: MERGED_RESULT,
        },
        CdcScenarioTestCase {
            description: "merge: stopped after the first table committed",
            tag: "17-alpine",
            steps: &APP_AFTER_FIRST,
            check_query: MERGED_QUERY,
            expected_result: MERGED_RESULT,
        },
        CdcScenarioTestCase {
            description: "merge: stopped after every table committed, before the slot confirmed",
            tag: "17-alpine",
            steps: &APP_BEFORE_CONFIRM,
            check_query: MERGED_QUERY,
            expected_result: MERGED_RESULT,
        },
        CdcScenarioTestCase {
            description: "changelog: stopped during a batch, before any table commits",
            tag: "17-alpine",
            steps: &LOG_BEFORE_COMMIT,
            check_query: CHANGELOG_QUERY,
            expected_result: CHANGELOG_RESULT,
        },
        CdcScenarioTestCase {
            description: "changelog: stopped after the first table committed, whose replay is skipped",
            tag: "17-alpine",
            steps: &LOG_AFTER_FIRST,
            check_query: CHANGELOG_QUERY,
            expected_result: CHANGELOG_RESULT,
        },
        CdcScenarioTestCase {
            description: "changelog: stopped before the slot confirmed, whose replay is skipped",
            tag: "17-alpine",
            steps: &LOG_BEFORE_CONFIRM,
            check_query: CHANGELOG_QUERY,
            expected_result: CHANGELOG_RESULT,
        },
        CdcScenarioTestCase {
            description: "a snapshot stopped part-way, and a dropped slot, both need --resnapshot",
            tag: "17-alpine",
            steps: &SNAPSHOT_CRASH,
            check_query: "SELECT string_agg(id || ':' || _sqlload_op, ',' ORDER BY id) FROM raw.app__orders",
            expected_result: "1:snapshot,3:snapshot,4:snapshot",
        },
    ];
    for test_case in test_cases {
        run_async(async {
            let postgres = start_postgres_version(test_case.tag).await?;
            let client = connect(&postgres).await?;
            client
                .batch_execute(SETUP)
                .await
                .map_err(|error| error.to_string())?;
            let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
            let database = folder.path().join("out.duckdb");
            let config = folder.path().join("sqlload.toml");
            std::fs::write(&config, cdc_project(postgres.port, &database))
                .map_err(|error| error.to_string())?;
            for (index, step) in test_case.steps.iter().enumerate() {
                client
                    .batch_execute(step.source_change)
                    .await
                    .map_err(|error| error.to_string())?;
                let (success, output) = run_sqlload(&config, step.args, step.crash_at)?;
                assert_eq!(
                    success, step.expected_success,
                    "{}: step {index} {:?}\n{output}",
                    test_case.description, step.args
                );
                assert!(
                    output.contains(step.expected_output),
                    "{}: step {index} {:?} output lacks {:?}\n{output}",
                    test_case.description,
                    step.args,
                    step.expected_output
                );
            }
            assert_eq!(
                duckdb_text(&database, test_case.check_query)?,
                test_case.expected_result,
                "{}",
                test_case.description
            );
            Ok(())
        })?;
    }
    Ok(())
}
