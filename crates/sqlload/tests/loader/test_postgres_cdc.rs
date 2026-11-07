use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres_version::start_postgres_version;

use crate::helpers::{
    CDC_CHANGES, cdc_project, crashed_batch, duckdb_text, run_async, run_sqlload, step,
};
use crate::test_types::{CdcScenarioTestCase, CdcStep, QuietSlotTestCase};

const SETUP: &str = "
CREATE TABLE public.orders (id int8 PRIMARY KEY, qty int4, big text, tags text[]);
CREATE TABLE public.items (id int8 PRIMARY KEY, name text, w real DEFAULT 1.1);
CREATE SCHEMA other;
CREATE TABLE other.audit (n int8);
CREATE SCHEMA docs;
CREATE TABLE docs.pages (id int8 PRIMARY KEY, body json);
INSERT INTO public.orders VALUES
  (1, 1, (SELECT string_agg(md5(g::text), '') FROM generate_series(1, 300) g), '{a,b}'),
  (2, 2, 'b', '{c}'), (3, 3, 'c', NULL);
INSERT INTO public.items VALUES (1, 'one'), (2, 'two');
";

const CHANGES: &str = CDC_CHANGES;

const MERGED_QUERY: &str = "SELECT (SELECT string_agg(id || ':' || qty || ':' || length(big) || ':' || COALESCE(tags, '-') || ':' || _sqlload_op || ':' || (_sqlload_deleted_at IS NOT NULL), ',' ORDER BY id) FROM raw.app__orders) || ' | ' || (SELECT string_agg(id || ':' || name || ':' || _sqlload_op, ',' ORDER BY id) FROM raw.app__items)";
const MERGED_RESULT: &str = "1:10:9600:{a,b}:update:false,2:2:1:{c}:delete:true,3:3:1:-:snapshot:false,4:4:1:{\"x y\",z}:insert:false | 1:one!:update,2:two:snapshot,3:three:insert";

const CHANGELOG_QUERY: &str = "SELECT (SELECT string_agg(_sqlload_seq || ':' || _sqlload_op || ':' || id, ',' ORDER BY _sqlload_lsn, _sqlload_seq, id) FROM raw.log__orders) || ' | ' || (SELECT string_agg(_sqlload_op || ':' || id, ',' ORDER BY _sqlload_lsn, _sqlload_seq, id) FROM raw.log__items) || ' | ' || (SELECT string_agg(_sqlload_unchanged, ';') FROM raw.log__orders WHERE _sqlload_op = 'update')";
const CHANGELOG_RESULT: &str = "0:snapshot:1,0:snapshot:2,0:snapshot:3,1:insert:4,2:update:1,1:delete:2 | snapshot:1,snapshot:2,insert:3,update:1 | ,big,";

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

const SNAPSHOT_CRASH: [CdcStep; 8] = [
    step("", APP, "snapshot-after-first-table", (false, "")),
    step("", APP, "", (false, "no snapshot from slot sqlload_app")),
    step(
        "",
        &["load", "app", "--resnapshot"],
        "",
        (
            false,
            "--resnapshot rebuilds app__items, app__orders from the whole source",
        ),
    ),
    step("", RESNAPSHOT, "", (true, "loaded")),
    step(CHANGES, APP, "", (true, "Completed")),
    step(
        "SELECT pg_drop_replication_slot('sqlload_app')",
        APP,
        "",
        (false, "slot sqlload_app is gone"),
    ),
    step("", RESNAPSHOT, "", (true, "loaded")),
    step("", APP, "", (true, "0 rows")),
];

const UNPUBLISHED_WRITES: &str = "INSERT INTO other.audit SELECT g FROM generate_series(1, 1000) g";

const LARGE_TRANSACTION: &str = "
BEGIN;
INSERT INTO public.orders SELECT g, g, 'x', NULL FROM generate_series(10, 40009) g;
UPDATE public.orders SET qty = qty + 1 WHERE id % 2 = 0;
DELETE FROM public.orders WHERE id % 5 = 0;
UPDATE public.orders SET id = id + 100000 WHERE id BETWEEN 10 AND 29;
COMMIT;
";

const SPILLED_LARGE: [CdcStep; 3] = [
    step("", BOTH, "", (true, "loaded")),
    step(
        LARGE_TRANSACTION,
        BOTH,
        "",
        (true, "spilling it to local disk"),
    ),
    step("", BOTH, "", (true, "0 rows")),
];

const SPILLED_CHANGES: [CdcStep; 3] = [
    step("", BOTH, "", (true, "loaded")),
    step(
        CHANGES,
        BOTH,
        "",
        (true, "spilling its changes to local disk"),
    ),
    step("", BOTH, "", (true, "0 rows")),
];

const SPILLED_LIVE_QUERY: &str = "SELECT (SELECT count(*) || ':' || sum(qty) || ':' || sum(id) FROM raw.app__orders WHERE _sqlload_deleted_at IS NULL) || ' | ' || (SELECT count(DISTINCT (_sqlload_lsn, _sqlload_seq)) || '/' || count(*) || ':' || max(_sqlload_seq) FROM raw.log__orders WHERE _sqlload_op <> 'snapshot')";

const SLOT_ADVANCED: &str = "SELECT confirmed_flush_lsn >= $1::text::pg_lsn FROM pg_replication_slots WHERE slot_name = 'sqlload_app'";

const RESNAPSHOT: &[&str] = &["load", "app", "--resnapshot", "--yes"];

const TRUNCATE_REINSERT: [CdcStep; 3] = [
    step("", APP, "", (true, "loaded")),
    step(
        "BEGIN; TRUNCATE public.orders; INSERT INTO public.orders VALUES (1, 7, 'z', NULL); COMMIT;",
        APP,
        "",
        (true, "Completed"),
    ),
    step("", APP, "", (true, "0 rows")),
];

const HARD: &[&str] = &["load", "hard"];

const HARD_SOURCE: &str = r#"
[sources.hard]
kind = "postgres_cdc"
connection = "pg"
include = ["public.*"]
deletes = "hard"

[sources.hard.tables."public.orders"]
description = "Orders, captured."
columns = { tags = "text" }
"#;

const HARD_KEY_CHANGE: [CdcStep; 3] = [
    step(
        "ALTER TABLE public.orders REPLICA IDENTITY FULL",
        HARD,
        "",
        (true, "loaded"),
    ),
    step(
        "DELETE FROM public.orders WHERE id = 2; UPDATE public.orders SET id = 5 WHERE id = 1;",
        HARD,
        "",
        (true, "Completed"),
    ),
    step("", HARD, "", (true, "0 rows")),
];

const KEY_CHANGE_WITHOUT_OLD_ROW: [CdcStep; 2] = [
    step("", APP, "", (true, "loaded")),
    step(
        "UPDATE public.orders SET id = 6 WHERE id = 1",
        APP,
        "",
        (
            false,
            "an update changed a row's key and left a large (TOASTed) value out",
        ),
    ),
];

const PARTIAL_LOADS: [CdcStep; 5] = [
    step("", APP, "", (true, "loaded")),
    step(
        CHANGES,
        &["load", "--select", "app__orders"],
        "",
        (
            false,
            "this selection leaves out public.items of source app",
        ),
    ),
    step(
        "",
        &["load", "app", "--exclude", "app__items"],
        "",
        (
            false,
            "this selection leaves out public.items of source app",
        ),
    ),
    step(
        "ALTER TABLE public.items ALTER COLUMN w TYPE text",
        APP,
        "",
        (false, "source app does not load while"),
    ),
    step(
        "ALTER TABLE public.items ALTER COLUMN w TYPE real USING w::real",
        APP,
        "",
        (true, "Completed"),
    ),
];

const FIRST_LOAD_PARTIAL: [CdcStep; 2] = [
    step(
        "",
        &["load", "--select", "app__orders"],
        "",
        (
            false,
            "this selection leaves out public.items of source app",
        ),
    ),
    step("", APP, "", (true, "loaded")),
];

const SCHEMA_CHANGE: [CdcStep; 2] = [
    step("", APP, "", (true, "loaded")),
    step(
        "ALTER TABLE public.items ADD COLUMN c int4 DEFAULT 5",
        APP,
        "",
        (false, "column c of app__items was added with a default"),
    ),
];

const ADD_COLUMN: &str = "UPDATE public.items SET name = 'one!' WHERE id = 1; ALTER TABLE public.items ADD COLUMN c int4; INSERT INTO public.items VALUES (3, 'three', 1.1, 7); UPDATE public.items SET c = 8 WHERE id = 2;";

const COLUMN_ADDED: [CdcStep; 3] = [
    step("", BOTH, "", (true, "loaded")),
    step(ADD_COLUMN, BOTH, "", (true, "Completed")),
    step("", BOTH, "", (true, "0 rows")),
];

const COLUMN_ADDED_QUERY: &str = "SELECT (SELECT string_agg(id || ':' || name || ':' || COALESCE(CAST(c AS VARCHAR), '-'), ',' ORDER BY id) FROM raw.app__items) || ' | ' || (SELECT string_agg(_sqlload_op || ':' || id || ':' || COALESCE(CAST(c AS VARCHAR), '-') || ':' || COALESCE(_sqlload_unchanged, '-'), ',' ORDER BY _sqlload_lsn, _sqlload_seq) FROM raw.log__items WHERE _sqlload_op <> 'snapshot')";

const COLUMN_ADDED_CRASH: [CdcStep; 4] = [
    step("", APP, "", (true, "loaded")),
    step(ADD_COLUMN, APP, "batch-before-commit", (false, "")),
    step("", APP, "", (true, "Completed")),
    step("", APP, "", (true, "0 rows")),
];

const COLUMN_ADDED_UNWRITTEN: [CdcStep; 3] = [
    step("", APP, "", (true, "loaded")),
    step(
        "UPDATE public.items SET name = 'one!' WHERE id = 1; ALTER TABLE public.items ADD COLUMN c int4;",
        APP,
        "",
        (true, "Completed"),
    ),
    step("", APP, "", (true, "0 rows")),
];

const IDENTITY_ADDED: [CdcStep; 2] = [
    step("", APP, "", (true, "loaded")),
    step(
        "ALTER TABLE public.items ADD COLUMN n int8 GENERATED ALWAYS AS IDENTITY",
        APP,
        "",
        (
            false,
            "column n of app__items was added with a default (or as an identity",
        ),
    ),
];

const COLUMN_READDED: [CdcStep; 3] = [
    step("", APP, "", (true, "loaded")),
    step(
        "ALTER TABLE public.items DROP COLUMN name",
        APP,
        "",
        (true, "Completed"),
    ),
    step(
        "ALTER TABLE public.items ADD COLUMN name text",
        APP,
        "",
        (
            false,
            "app__items loads column name again after it stopped loading",
        ),
    ),
];

const COLUMN_DROPPED_INSIDE: [CdcStep; 3] = [
    step("", BOTH, "", (true, "loaded")),
    step(
        "BEGIN; UPDATE public.items SET w = 2.5 WHERE id = 1; ALTER TABLE public.items DROP COLUMN name; INSERT INTO public.items VALUES (3, 3.5); COMMIT;",
        BOTH,
        "",
        (true, "Completed"),
    ),
    step("", BOTH, "", (true, "0 rows")),
];

const COLUMN_DROPPED_QUERY: &str = "SELECT (SELECT string_agg(id || ':' || COALESCE(name, '-') || ':' || CAST(CAST(w AS DECIMAL(3, 1)) AS VARCHAR), ',' ORDER BY id) FROM raw.app__items) || ' | ' || (SELECT string_agg(_sqlload_op || ':' || id || ':' || CAST(CAST(w AS DECIMAL(3, 1)) AS VARCHAR), ',' ORDER BY _sqlload_lsn, _sqlload_seq) FROM raw.log__items WHERE _sqlload_op <> 'snapshot')";

const COLUMN_RENAMED: [CdcStep; 2] = [
    step("", APP, "", (true, "loaded")),
    step(
        "ALTER TABLE public.items RENAME COLUMN name TO title",
        APP,
        "",
        (
            false,
            "app__items now loads column title, which the table already had at its snapshot",
        ),
    ),
];

const KEY_CHANGED: [CdcStep; 3] = [
    step("", BOTH, "", (true, "loaded")),
    step(
        "ALTER TABLE public.items DROP CONSTRAINT items_pkey; ALTER TABLE public.items ADD PRIMARY KEY (id, name); INSERT INTO public.items VALUES (1, 'uno', 1.1);",
        LOG,
        "",
        (true, "Completed"),
    ),
    step(
        "",
        APP,
        "",
        (false, "the key of app__items changed since its snapshot"),
    ),
];

const TYPE_WIDENED: [CdcStep; 3] = [
    step("", APP, "", (true, "loaded")),
    step(
        "UPDATE public.orders SET qty = 6 WHERE id = 2; ALTER TABLE public.orders ALTER COLUMN qty TYPE int8; UPDATE public.orders SET qty = 5000000000 WHERE id = 3;",
        APP,
        "",
        (true, "Completed"),
    ),
    step("", APP, "", (true, "0 rows")),
];

const NOKEY_SOURCE: &str = r#"
[sources.nokey]
kind = "postgres_cdc"
connection = "pg"
include = ["other.*"]
cdc_mode = "changelog"
"#;

const NO_PRIMARY_KEY: [CdcStep; 1] = [step(
    "",
    &["load", "nokey"],
    "",
    (false, "other.audit has no primary key"),
)];

const FLOAT4: [CdcStep; 2] = [
    step("", APP, "", (true, "loaded")),
    step(
        "INSERT INTO public.items VALUES (4, 'four', 1.1)",
        APP,
        "",
        (true, "Completed"),
    ),
];

const UNIQUE_SOURCE: &str = r#"
[sources.uniq]
kind = "postgres_cdc"
connection = "pg"
include = ["public.items"]

[sources.uniq.tables."public.items"]
description = "Items, keyed by name."
unique_key = ["name"]
"#;

const KEY_OUTSIDE_IDENTITY: [CdcStep; 1] = [step(
    "",
    &["load", "uniq"],
    "",
    (false, "unique_key column name is not in the primary key"),
)];

const IDENTITY_NOTHING: [CdcStep; 2] = [
    step(
        "ALTER TABLE public.items REPLICA IDENTITY NOTHING",
        APP,
        "",
        (false, "\"public\".\"items\" has REPLICA IDENTITY NOTHING"),
    ),
    step(
        "ALTER TABLE public.items REPLICA IDENTITY DEFAULT",
        APP,
        "",
        (true, "loaded"),
    ),
];

const SHARED_SOURCE: &str = r#"
[sources.narrow]
kind = "postgres_cdc"
connection = "pg"
include = ["public.orders"]
publication = "shared_pub"

[sources.narrow.tables."public.orders"]
description = "Orders, captured through a shared publication."
columns = { tags = "text" }
"#;

const EXTRA_PUBLISHED: [CdcStep; 5] = [
    step(
        "CREATE PUBLICATION shared_pub FOR TABLE public.orders, public.items",
        &["load", "narrow"],
        "",
        (
            false,
            "publication shared_pub also publishes \"public\".\"items\"",
        ),
    ),
    step("", APP, "", (true, "loaded")),
    step(
        "ALTER PUBLICATION sqlload_app ADD TABLE other.audit",
        APP,
        "",
        (
            false,
            "publication sqlload_app also publishes \"other\".\"audit\"",
        ),
    ),
    step("", RESNAPSHOT, "", (true, "loaded")),
    step(CHANGES, APP, "", (true, "Completed")),
];

const JSON_SOURCE: &str = r#"
[sources.pages]
kind = "postgres_cdc"
connection = "pg"
include = ["docs.*"]
"#;

const UNPINNED_JSON: [CdcStep; 1] = [step(
    "",
    &["load", "pages"],
    "",
    (false, "streams json values as the source wrote them"),
)];

const STRICT_SOURCE: &str = r#"
[sources.strict]
kind = "postgres_cdc"
connection = "pg"
include = ["public.orders"]

[sources.strict.tables."public.orders"]
description = "Orders, with only some columns declared."
contract = "enforced"
columns = { id = "bigint", big = "text", tags = "text" }
"#;

const STRICT: &[&str] = &["load", "strict"];

const DROPPED_COLUMN_TOAST: [CdcStep; 2] = [
    step("", STRICT, "", (true, "loaded")),
    step(
        "UPDATE public.orders SET qty = 11, tags = '{q}' WHERE id = 1",
        STRICT,
        "",
        (true, "Completed"),
    ),
];

/// Every cell of D296's restart table, each in a fresh server: the process stops itself at the point and a later load must leave each table exactly as the source has it, with no change lost or applied twice (a changelog would show a replayed change as a duplicate row).
#[test]
fn given_a_postgres_cdc_source_when_loads_stop_at_each_restart_point_then_the_tables_converge_without_duplicates()
-> Result<(), String> {
    let test_cases = [
        CdcScenarioTestCase {
            description: "Postgres 15: snapshot, both transactions, then a re-run of nothing",
            tag: "15-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &CAUGHT_UP,
            check_query: MERGED_QUERY,
            expected_result: MERGED_RESULT,
        },
        CdcScenarioTestCase {
            description: "Postgres 16: the changelog holds every change once",
            tag: "16-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &CAUGHT_UP,
            check_query: CHANGELOG_QUERY,
            expected_result: CHANGELOG_RESULT,
        },
        CdcScenarioTestCase {
            description: "Postgres 17: merge keeps the unchanged TOASTed value",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &CAUGHT_UP,
            check_query: MERGED_QUERY,
            expected_result: MERGED_RESULT,
        },
        CdcScenarioTestCase {
            description: "merge: stopped during a batch, before any table commits",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &APP_BEFORE_COMMIT,
            check_query: MERGED_QUERY,
            expected_result: MERGED_RESULT,
        },
        CdcScenarioTestCase {
            description: "merge: stopped after the first table committed",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &APP_AFTER_FIRST,
            check_query: MERGED_QUERY,
            expected_result: MERGED_RESULT,
        },
        CdcScenarioTestCase {
            description: "merge: stopped after every table committed, before the slot confirmed",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &APP_BEFORE_CONFIRM,
            check_query: MERGED_QUERY,
            expected_result: MERGED_RESULT,
        },
        CdcScenarioTestCase {
            description: "changelog: stopped during a batch, before any table commits",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &LOG_BEFORE_COMMIT,
            check_query: CHANGELOG_QUERY,
            expected_result: CHANGELOG_RESULT,
        },
        CdcScenarioTestCase {
            description: "changelog: stopped after the first table committed, whose replay is skipped",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &LOG_AFTER_FIRST,
            check_query: CHANGELOG_QUERY,
            expected_result: CHANGELOG_RESULT,
        },
        CdcScenarioTestCase {
            description: "changelog: stopped before the slot confirmed, whose replay is skipped",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &LOG_BEFORE_CONFIRM,
            check_query: CHANGELOG_QUERY,
            expected_result: CHANGELOG_RESULT,
        },
        CdcScenarioTestCase {
            description: "a snapshot stopped part-way, and a dropped slot, both need --resnapshot",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &SNAPSHOT_CRASH,
            check_query: "SELECT string_agg(id || ':' || _sqlload_op, ',' ORDER BY id) FROM raw.app__orders",
            expected_result: "1:snapshot,3:snapshot,4:snapshot",
        },
        CdcScenarioTestCase {
            description: "snapshot rows sit below the slot's consistent point, so a commit at exactly that point still applies",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &CAUGHT_UP,
            check_query: "SELECT CAST((SELECT max(_sqlload_lsn) FROM raw.app__orders WHERE _sqlload_op = 'snapshot') < (SELECT max(snapshot_lsn) FROM raw._sqlload_cdc_snapshots) AS VARCHAR)",
            expected_result: "true",
        },
        CdcScenarioTestCase {
            description: "soft deletes: a truncate then a reinsert in one transaction keeps the reinserted row",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &TRUNCATE_REINSERT,
            check_query: "SELECT string_agg(id || ':' || qty || ':' || _sqlload_op || ':' || (_sqlload_deleted_at IS NOT NULL), ',' ORDER BY id) FROM raw.app__orders",
            expected_result: "1:7:insert:false,2:2:truncate:true,3:3:truncate:true",
        },
        CdcScenarioTestCase {
            description: "hard deletes remove rows, and a key change under REPLICA IDENTITY FULL keeps its TOASTed value",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: HARD_SOURCE,
            steps: &HARD_KEY_CHANGE,
            check_query: "SELECT string_agg(id || ':' || length(big) || ':' || _sqlload_op, ',' ORDER BY id) || ':' || (SELECT count(*) FROM information_schema.columns WHERE table_name = 'hard__orders' AND column_name = '_sqlload_deleted_at') FROM raw.hard__orders",
            expected_result: "3:1:snapshot,5:9600:update:0",
        },
        CdcScenarioTestCase {
            description: "a key change that leaves a TOASTed value out stops the load instead of writing NULL",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &KEY_CHANGE_WITHOUT_OLD_ROW,
            check_query: "SELECT string_agg(id || ':' || length(big), ',' ORDER BY id) FROM raw.app__orders",
            expected_result: "1:9600,2:1,3:1",
        },
        CdcScenarioTestCase {
            description: "a load of part of the source, or with a table blocked, is refused, and the left-out changes still arrive",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &PARTIAL_LOADS,
            check_query: MERGED_QUERY,
            expected_result: MERGED_RESULT,
        },
        CdcScenarioTestCase {
            description: "a transaction larger than memory spills to disk: merge matches Postgres, and the changelog, staged in several chunks, holds every change once at its position",
            tag: "17-alpine",
            spill_bytes: "100000",
            extra_config: "",
            steps: &SPILLED_LARGE,
            check_query: SPILLED_LIVE_QUERY,
            expected_result: "32003:640336007:641920006 | 68017/68017:68017",
        },
        CdcScenarioTestCase {
            description: "spilled merge: the TOASTed value, the delete and the second table come out as in memory",
            tag: "15-alpine",
            spill_bytes: "1",
            extra_config: "",
            steps: &SPILLED_CHANGES,
            check_query: MERGED_QUERY,
            expected_result: MERGED_RESULT,
        },
        CdcScenarioTestCase {
            description: "spilled changelog: every change once, in order, with its unchanged flags",
            tag: "16-alpine",
            spill_bytes: "1",
            extra_config: "",
            steps: &SPILLED_CHANGES,
            check_query: CHANGELOG_QUERY,
            expected_result: CHANGELOG_RESULT,
        },
        CdcScenarioTestCase {
            description: "spilled: a truncate then a reinsert in one transaction keeps the reinserted row",
            tag: "17-alpine",
            spill_bytes: "1",
            extra_config: "",
            steps: &TRUNCATE_REINSERT,
            check_query: "SELECT string_agg(id || ':' || qty || ':' || _sqlload_op || ':' || (_sqlload_deleted_at IS NOT NULL), ',' ORDER BY id) FROM raw.app__orders",
            expected_result: "1:7:insert:false,2:2:truncate:true,3:3:truncate:true",
        },
        CdcScenarioTestCase {
            description: "spilled: a key change deletes the old key in its own partition and keeps its TOASTed value",
            tag: "17-alpine",
            spill_bytes: "1",
            extra_config: HARD_SOURCE,
            steps: &HARD_KEY_CHANGE,
            check_query: "SELECT string_agg(id || ':' || length(big) || ':' || _sqlload_op, ',' ORDER BY id) || ':' || (SELECT count(*) FROM information_schema.columns WHERE table_name = 'hard__orders' AND column_name = '_sqlload_deleted_at') FROM raw.hard__orders",
            expected_result: "3:1:snapshot,5:9600:update:0",
        },
        CdcScenarioTestCase {
            description: "spilled: stopped after the first table committed, the replay converges",
            tag: "17-alpine",
            spill_bytes: "1",
            extra_config: "",
            steps: &APP_AFTER_FIRST,
            check_query: MERGED_QUERY,
            expected_result: MERGED_RESULT,
        },
        CdcScenarioTestCase {
            description: "a first load of part of the source is refused before it makes the slot, so the whole source snapshots after",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &FIRST_LOAD_PARTIAL,
            check_query: "SELECT string_agg(CAST(id AS VARCHAR), ',' ORDER BY id) FROM raw.app__items",
            expected_result: "1,2",
        },
        CdcScenarioTestCase {
            description: "a column added between loads stops the load instead of leaving old rows without its default",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &SCHEMA_CHANGE,
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM raw.app__items",
            expected_result: "2",
        },
        CdcScenarioTestCase {
            description: "a column added between changes loads: merge keeps older rows' live value, a changelog writes NULL and flags it",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &COLUMN_ADDED,
            check_query: COLUMN_ADDED_QUERY,
            expected_result: "1:one!:-,2:two:8,3:three:7 | update:1:-:,c,,insert:3:7:-,update:2:8:-",
        },
        CdcScenarioTestCase {
            description: "spilled: a column added between changes loads the same",
            tag: "15-alpine",
            spill_bytes: "1",
            extra_config: "",
            steps: &COLUMN_ADDED,
            check_query: COLUMN_ADDED_QUERY,
            expected_result: "1:one!:-,2:two:8,3:three:7 | update:1:-:,c,,insert:3:7:-,update:2:8:-",
        },
        CdcScenarioTestCase {
            description: "stopped after the destination gained the column, before the batch committed, the replay converges",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &COLUMN_ADDED_CRASH,
            check_query: "SELECT string_agg(id || ':' || name || ':' || COALESCE(CAST(c AS VARCHAR), '-'), ',' ORDER BY id) FROM raw.app__items",
            expected_result: "1:one!:-,2:two:8,3:three:7",
        },
        CdcScenarioTestCase {
            description: "a column added with no change after it loads the changes before it, not a rerun loop",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &COLUMN_ADDED_UNWRITTEN,
            check_query: "SELECT string_agg(id || ':' || name || ':' || COALESCE(CAST(c AS VARCHAR), '-'), ',' ORDER BY id) FROM raw.app__items",
            expected_result: "1:one!:-,2:two:-",
        },
        CdcScenarioTestCase {
            description: "an identity column added fills existing rows without changes, so it is refused",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &IDENTITY_ADDED,
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM raw.app__items",
            expected_result: "2",
        },
        CdcScenarioTestCase {
            description: "a column dropped, then added back under its name, is refused, since its destination values are stale",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &COLUMN_READDED,
            check_query: "SELECT string_agg(id || ':' || name, ',' ORDER BY id) FROM raw.app__items",
            expected_result: "1:one,2:two",
        },
        CdcScenarioTestCase {
            description: "a middle column dropped inside a transaction keeps each change's values in their columns",
            tag: "16-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &COLUMN_DROPPED_INSIDE,
            check_query: COLUMN_DROPPED_QUERY,
            expected_result: "1:one:2.5,2:two:1.1,3:-:3.5 | update:1:2.5,insert:3:3.5",
        },
        CdcScenarioTestCase {
            description: "a renamed column is refused, since its rows hold values no change carries",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &COLUMN_RENAMED,
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM raw.app__items",
            expected_result: "2",
        },
        CdcScenarioTestCase {
            description: "a primary key change loads into a changelog and is refused under merge",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &KEY_CHANGED,
            check_query: "SELECT string_agg(_sqlload_op || ':' || id || ':' || name, ',' ORDER BY _sqlload_lsn, _sqlload_seq) FROM raw.log__items WHERE _sqlload_op <> 'snapshot'",
            expected_result: "insert:1:uno",
        },
        CdcScenarioTestCase {
            description: "a column widened between changes loads both sides",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &TYPE_WIDENED,
            check_query: "SELECT string_agg(id || ':' || qty, ',' ORDER BY id) FROM raw.app__orders",
            expected_result: "1:1,2:6,3:5000000000",
        },
        CdcScenarioTestCase {
            description: "a table without a primary key is refused before any publication is made",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: NOKEY_SOURCE,
            steps: &NO_PRIMARY_KEY,
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_name = 'nokey__audit'",
            expected_result: "0",
        },
        CdcScenarioTestCase {
            description: "a unique_key outside the primary key is refused before the snapshot, instead of losing deletes",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: UNIQUE_SOURCE,
            steps: &KEY_OUTSIDE_IDENTITY,
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_name = 'uniq__items'",
            expected_result: "0",
        },
        CdcScenarioTestCase {
            description: "a table with REPLICA IDENTITY NOTHING is refused before it is published",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &IDENTITY_NOTHING,
            check_query: "SELECT string_agg(CAST(id AS VARCHAR), ',' ORDER BY id) FROM raw.app__items",
            expected_result: "1,2",
        },
        CdcScenarioTestCase {
            description: "a publication with tables the source does not load is refused, and SQLLoad's own follows the source again on --resnapshot",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: SHARED_SOURCE,
            steps: &EXTRA_PUBLISHED,
            check_query: MERGED_QUERY,
            expected_result: MERGED_RESULT,
        },
        CdcScenarioTestCase {
            description: "a json column must be pinned to text, since the stream keeps the source's spelling",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: JSON_SOURCE,
            steps: &UNPINNED_JSON,
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_name = 'pages__pages'",
            expected_result: "0",
        },
        CdcScenarioTestCase {
            description: "a real value streams as the snapshot reads it",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &FLOAT4,
            check_query: "SELECT count(DISTINCT w) || ':' || count(w) FROM raw.app__items",
            expected_result: "1:3",
        },
        CdcScenarioTestCase {
            description: "a column the enforced contract leaves out does not shift which values count as unchanged",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: STRICT_SOURCE,
            steps: &DROPPED_COLUMN_TOAST,
            check_query: "SELECT string_agg(id || ':' || length(big) || ':' || tags, ',' ORDER BY id) FROM raw.strict__orders WHERE id = 1",
            expected_result: "1:9600:{q}",
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
            let project = cdc_project(postgres.port, &database) + test_case.extra_config;
            std::fs::write(&config, project).map_err(|error| error.to_string())?;
            for (index, step) in test_case.steps.iter().enumerate() {
                client
                    .batch_execute(step.source_change)
                    .await
                    .map_err(|error| error.to_string())?;
                let (success, output) =
                    run_sqlload(&config, step.args, (step.crash_at, test_case.spill_bytes))?;
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

/// Postgres 15 and later send nothing for transactions on tables the slot does not publish, so a load of a quiet source must still confirm up to where it stopped, or the slot would hold the server's WAL forever.
#[test]
fn given_a_quiet_postgres_cdc_source_when_other_tables_write_then_a_load_advances_the_slot()
-> Result<(), String> {
    let test_cases = [
        QuietSlotTestCase {
            description: "Postgres 15 skips the unpublished transactions",
            tag: "15-alpine",
            expected_advanced: (true, true),
        },
        QuietSlotTestCase {
            description: "Postgres 17 skips the unpublished transactions",
            tag: "17-alpine",
            expected_advanced: (true, true),
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
            let config = folder.path().join("sqlload.toml");
            std::fs::write(
                &config,
                cdc_project(postgres.port, &folder.path().join("out.duckdb")),
            )
            .map_err(|error| error.to_string())?;
            run_sqlload(&config, APP, ("", ""))?;
            client
                .batch_execute(UNPUBLISHED_WRITES)
                .await
                .map_err(|error| error.to_string())?;
            let written: String = client
                .query_one("SELECT pg_current_wal_lsn()::text", &[])
                .await
                .map_err(|error| error.to_string())?
                .get(0);
            let (success, output) = run_sqlload(&config, APP, ("", ""))?;
            let advanced: bool = client
                .query_one(SLOT_ADVANCED, &[&written])
                .await
                .map_err(|error| error.to_string())?
                .get(0);
            assert_eq!(
                (success, advanced),
                test_case.expected_advanced,
                "{}\n{output}",
                test_case.description
            );
            Ok(())
        })?;
    }
    Ok(())
}
