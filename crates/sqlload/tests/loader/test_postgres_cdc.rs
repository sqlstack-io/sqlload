use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres_version::start_postgres_version;

use crate::helpers::{
    CDC_CHANGES, cdc_project, crashed_batch, duckdb_text, run_async, run_sqlload, spawn_sqlload,
    step, stop_sqlload,
};
use crate::test_types::{CdcScenarioTestCase, CdcStep, FollowTestCase, QuietSlotTestCase};

pub(crate) const SETUP: &str = "
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

pub(crate) const CHANGES: &str = CDC_CHANGES;

const MERGED_QUERY: &str = "SELECT (SELECT string_agg(id || ':' || qty || ':' || length(big) || ':' || COALESCE(tags, '-') || ':' || _sqlload_op || ':' || (_sqlload_deleted_at IS NOT NULL), ',' ORDER BY id) FROM raw.app__orders) || ' | ' || (SELECT string_agg(id || ':' || name || ':' || _sqlload_op, ',' ORDER BY id) FROM raw.app__items)";
const MERGED_RESULT: &str = "1:10:9600:{a,b}:update:false,2:2:1:{c}:delete:true,3:3:1:-:snapshot:false,4:4:1:{\"x y\",z}:insert:false | 1:one!:update,2:two:snapshot,3:three:insert";

const CHANGELOG_QUERY: &str = "SELECT (SELECT string_agg(_sqlload_seq || ':' || _sqlload_op || ':' || id, ',' ORDER BY _sqlload_lsn, _sqlload_seq, id) FROM raw.log__orders) || ' | ' || (SELECT string_agg(_sqlload_op || ':' || id, ',' ORDER BY _sqlload_lsn, _sqlload_seq, id) FROM raw.log__items) || ' | ' || (SELECT string_agg(_sqlload_unchanged, ';') FROM raw.log__orders WHERE _sqlload_op = 'update')";
pub(crate) const CHANGELOG_RESULT: &str = "0:snapshot:1,0:snapshot:2,0:snapshot:3,1:insert:4,2:update:1,1:delete:2 | snapshot:1,snapshot:2,insert:3,update:1 | ,big,";

const APP: &[&str] = &["load", "app"];
pub(crate) const LOG: &[&str] = &["load", "log"];
const BOTH: &[&str] = &["load", "app", "log"];

const CAUGHT_UP: [CdcStep; 4] = [
    step("", BOTH, "", (true, "loaded")),
    step(
        "",
        &["load", "app", "log", "--reload", "--yes"],
        "",
        (
            false,
            "a postgres_cdc table is copied again with --resnapshot, not --reload",
        ),
    ),
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
    step("", APP, "", (true, "Completed")),
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

pub(crate) const LARGE_TRANSACTION: &str = "
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

pub(crate) const ADD_COLUMN: &str = "UPDATE public.items SET name = 'one!' WHERE id = 1; ALTER TABLE public.items ADD COLUMN c int4; INSERT INTO public.items VALUES (3, 'three', 1.1, 7); UPDATE public.items SET c = 8 WHERE id = 2;";

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

const UNSTREAMED_SOURCES: &str = r#"
[sources.gen]
kind = "postgres_cdc"
connection = "pg"
include = ["gen.*"]

[sources.gen.tables."gen.t"]
description = "A table with a generated column."

[sources.cols]
kind = "postgres_cdc"
connection = "pg"
include = ["cols.*"]
publication = "cols_pub"

[sources.cols.tables."cols.t"]
description = "A table published with a column list."
"#;

const UNSTREAMED: [CdcStep; 2] = [
    step(
        "CREATE SCHEMA gen; CREATE TABLE gen.t (id int8 PRIMARY KEY, n int4, d int4 GENERATED ALWAYS AS (n * 2) STORED); INSERT INTO gen.t (id, n) VALUES (1, 1);",
        &["load", "gen"],
        "",
        (
            false,
            "gen.t loads d, which logical replication does not send",
        ),
    ),
    step(
        "CREATE SCHEMA cols; CREATE TABLE cols.t (id int8 PRIMARY KEY, a text, b text); CREATE PUBLICATION cols_pub FOR TABLE cols.t (id, a);",
        &["load", "cols"],
        "",
        (
            false,
            "cols.t loads b, which logical replication does not send",
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

const NEW_TABLES: &str = "
CREATE TABLE public.extra (id int8 PRIMARY KEY, v text); COMMENT ON TABLE public.extra IS 'Extra rows.';
CREATE TABLE public.extra2 (id int8 PRIMARY KEY, v text); COMMENT ON TABLE public.extra2 IS 'More extra rows.';
INSERT INTO public.extra VALUES (1, 'a'); INSERT INTO public.extra2 VALUES (1, 'x');
";

const NO_BACKFILL_SLOT: &str = "DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'sqlload_app_backfill') THEN RAISE EXCEPTION 'the temporary backfill slot is still there'; END IF; END $$;";

const BACKFILL_MERGE: [CdcStep; 6] = [
    step("", APP, "", (true, "loaded")),
    step(CHANGES, APP, "", (true, "Completed")),
    step(NEW_TABLES, APP, "backfill-after-first-table", (false, "")),
    step(
        "UPDATE public.orders SET qty = 11 WHERE id = 3; UPDATE public.items SET name = 'two!' WHERE id = 2;",
        APP,
        "",
        (true, "Completed"),
    ),
    step(
        "UPDATE public.extra SET v = 'b' WHERE id = 1; INSERT INTO public.extra2 VALUES (2, 'y');",
        APP,
        "",
        (true, "Completed"),
    ),
    step(NO_BACKFILL_SLOT, APP, "", (true, "0 rows")),
];

const BACKFILL_MERGE_QUERY: &str = "SELECT (SELECT string_agg(id || ':' || qty || ':' || length(big) || ':' || COALESCE(tags, '-') || ':' || _sqlload_op || ':' || (_sqlload_deleted_at IS NOT NULL), ',' ORDER BY id) FROM raw.app__orders) || ' | ' || (SELECT string_agg(id || ':' || name || ':' || _sqlload_op, ',' ORDER BY id) FROM raw.app__items) || ' | ' || (SELECT string_agg(id || ':' || v || ':' || _sqlload_op, ',' ORDER BY id) FROM raw.app__extra) || ' | ' || (SELECT string_agg(id || ':' || v || ':' || _sqlload_op, ',' ORDER BY id) FROM raw.app__extra2)";

pub(crate) const BACKFILL_CHANGELOG: [CdcStep; 5] = [
    step("", LOG, "", (true, "loaded")),
    step(
        "CREATE TABLE public.extra (id int8 PRIMARY KEY, v text); COMMENT ON TABLE public.extra IS 'Extra rows.';",
        LOG,
        "backfill-after-publication",
        (false, ""),
    ),
    step(
        "INSERT INTO public.extra VALUES (1, 'a'), (2, 'b'), (3, 'c'); DELETE FROM public.extra;",
        LOG,
        "",
        (true, "Completed"),
    ),
    step(
        "INSERT INTO public.extra VALUES (9, 'z');",
        LOG,
        "",
        (true, "Completed"),
    ),
    step("", LOG, "", (true, "0 rows")),
];

const BACKFILL_RESTARTS: [CdcStep; 6] = [
    step("", APP, "", (true, "loaded")),
    step(
        NEW_TABLES,
        &["plan", "app"],
        "",
        (true, "snapshot copy, then apply changes"),
    ),
    step("", APP, "backfill-before-record", (false, "")),
    step(
        "UPDATE public.extra SET v = 'b' WHERE id = 1;",
        APP,
        "backfill-before-stream",
        (false, ""),
    ),
    step(
        "INSERT INTO public.extra VALUES (2, 'c');",
        APP,
        "",
        (true, "Completed"),
    ),
    step(
        "ALTER PUBLICATION sqlload_app DROP TABLE public.extra2",
        APP,
        "",
        (
            false,
            "publication sqlload_app no longer publishes \"public\".\"extra2\"",
        ),
    ),
];

const NAMED_SOURCE: &str = r#"
[sources.named]
kind = "postgres_cdc"
connection = "pg"
include = ["public.*"]
publication = "named_pub"

[sources.named.tables."public.orders"]
description = "Orders, captured through a named publication."
columns = { tags = "text" }
"#;

const NAMED: &[&str] = &["load", "named"];

const BACKFILL_NAMED: [CdcStep; 5] = [
    step(
        "CREATE PUBLICATION named_pub FOR TABLE public.orders, public.items",
        NAMED,
        "",
        (true, "loaded"),
    ),
    step(
        "CREATE TABLE public.extra (id int8 PRIMARY KEY, v text); COMMENT ON TABLE public.extra IS 'Extra rows.'; INSERT INTO public.extra VALUES (1, 'a');",
        NAMED,
        "",
        (
            false,
            "publication named_pub does not publish \"public\".\"extra\"",
        ),
    ),
    step(
        "INSERT INTO public.extra VALUES (2, 'b'); ALTER PUBLICATION named_pub ADD TABLE public.extra; INSERT INTO public.extra VALUES (3, 'c');",
        NAMED,
        "",
        (true, "Completed"),
    ),
    step(
        "UPDATE public.extra SET v = 'z' WHERE id = 1;",
        NAMED,
        "",
        (true, "Completed"),
    ),
    step("", NAMED, "", (true, "0 rows")),
];

pub(crate) const EXTRA_TABLE: &str = "CREATE TABLE public.extra (id int8 PRIMARY KEY, v text); COMMENT ON TABLE public.extra IS 'Extra rows.';";

const EXTRA_UNPUBLISHED: &str = "DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_publication_tables WHERE pubname = 'sqlload_app' AND tablename = 'extra') THEN RAISE EXCEPTION 'the refused load published extra'; END IF; END $$;";

const BACKFILL_REJOIN: [CdcStep; 8] = [
    step("", APP, "", (true, "loaded")),
    step(EXTRA_TABLE, APP, "", (true, "Completed")),
    step(
        "INSERT INTO public.extra VALUES (1, 'a');",
        APP,
        "",
        (true, "Completed"),
    ),
    step(
        "DROP TABLE public.extra;",
        APP,
        "",
        (false, "this load leaves out app__extra"),
    ),
    step("", RESNAPSHOT, "", (true, "loaded")),
    step(
        EXTRA_TABLE,
        APP,
        "",
        (
            false,
            "rows captured before slot sqlload_app was recreated are still in app__extra",
        ),
    ),
    step(EXTRA_UNPUBLISHED, &["plan", "app"], "", (true, "")),
    step("", APP, "", (false, "are still in app__extra")),
];

const BACKFILL_REJOIN_EMPTY: [CdcStep; 7] = [
    step("", APP, "", (true, "loaded")),
    step(EXTRA_TABLE, APP, "", (true, "Completed")),
    step(
        "DROP TABLE public.extra;",
        APP,
        "",
        (false, "this load leaves out app__extra"),
    ),
    step("", RESNAPSHOT, "", (true, "loaded")),
    step(
        "CREATE TABLE public.extra (id int8 PRIMARY KEY, v text); COMMENT ON TABLE public.extra IS 'Extra rows.'; INSERT INTO public.extra VALUES (5, 'e');",
        APP,
        "backfill-before-record",
        (false, ""),
    ),
    step(
        "INSERT INTO public.extra VALUES (6, 'f');",
        APP,
        "",
        (true, "Completed"),
    ),
    step(
        "UPDATE public.extra SET v = 'g' WHERE id = 6;",
        APP,
        "",
        (true, "Completed"),
    ),
];

const FOLLOW_APP: &[&str] = &["load", "app", "--follow", "--max-duration", "3s"];

const FOLLOW_LOG: &[&str] = &["load", "log", "--follow", "--max-duration", "3s"];

const FOLLOWED: [CdcStep; 3] = [
    step("", APP, "", (true, "loaded")),
    step(CHANGES, FOLLOW_APP, "", (true, "Completed")),
    step("", APP, "", (true, "0 rows")),
];

const FOLLOWED_LOG: [CdcStep; 3] = [
    step("", LOG, "", (true, "loaded")),
    step(CHANGES, FOLLOW_LOG, "", (true, "Completed")),
    step("", LOG, "", (true, "0 rows")),
];

const FOLLOW_AFTER_FIRST: [CdcStep; 5] = crashed_batch(FOLLOW_APP, "batch-after-first-table");

const FOLLOW_LOG_BEFORE_CONFIRM: [CdcStep; 5] = crashed_batch(FOLLOW_LOG, "batch-before-confirm");

const PLAIN_SOURCE: &str = r#"
[sources.plain]
kind = "postgres"
connection = "pg"
include = ["public.items"]

[sources.plain.tables."public.items"]
description = "Items, read whole."
"#;

const FOLLOW_REFUSED: [CdcStep; 4] = [
    step(
        "",
        &["load", "app", "log", "--follow"],
        "",
        (
            false,
            "load --follow follows one postgres_cdc source per process",
        ),
    ),
    step(
        "",
        &["load", "app", "plain", "--follow"],
        "",
        (
            false,
            "plain__items is not captured, so load them without --follow",
        ),
    ),
    step(
        "",
        &["plan", "app", "--follow"],
        "",
        (false, "--follow and --max-duration apply to load only"),
    ),
    step(
        "",
        &["load", "app", "--max-duration", "3s"],
        "",
        (false, "--follow"),
    ),
];

/// Waits up to 15 seconds for slot `sqlload_app` to stream.
const AWAIT_STREAMING: &str = "DO $$ BEGIN FOR attempt IN 1..150 LOOP EXIT WHEN EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'sqlload_app' AND active); PERFORM pg_sleep(0.1); END LOOP; END $$;";

/// Waits up to 6 seconds for slot `sqlload_app` to confirm the position that replaces `{lsn}`.
const AWAIT_ADVANCED: &str = "DO $$ BEGIN FOR attempt IN 1..60 LOOP EXIT WHEN (SELECT confirmed_flush_lsn >= '{lsn}'::pg_lsn FROM pg_replication_slots WHERE slot_name = 'sqlload_app'); PERFORM pg_sleep(0.1); END LOOP; END $$;";

const FOLLOW_SHORT: &[&str] = &["load", "app", "--follow", "--max-duration", "15s"];

const FIRST_CHANGES: &str = "BEGIN; INSERT INTO public.orders VALUES (4, 4, 'd', '{\"x y\",z}'); UPDATE public.orders SET qty = 10 WHERE id = 1; INSERT INTO public.items VALUES (3, 'three'); COMMIT;";

const SECOND_CHANGES: &str = "BEGIN; DELETE FROM public.orders WHERE id = 2; UPDATE public.items SET name = 'one!' WHERE id = 1; COMMIT; INSERT INTO other.audit SELECT g FROM generate_series(1, 1000) g;";

const ITEMS_WITH_C: &str = "SELECT string_agg(id || ':' || name || ':' || COALESCE(CAST(c AS VARCHAR), '-'), ',' ORDER BY id) FROM raw.app__items";

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

const NOT_OWNER_SOURCE: &str = r#"
[connections.reader]
kind = "postgres"
host = "127.0.0.1"
port = {port}
user = "reader"
password = "pw"
database = "app"

[sources.reader]
kind = "postgres_cdc"
connection = "reader"
include = ["public.items"]

[sources.reader.tables."public.items"]
description = "Items, captured."
"#;

const NOT_OWNER: [CdcStep; 1] = [step(
    "CREATE ROLE reader LOGIN REPLICATION PASSWORD 'pw'; GRANT SELECT ON ALL TABLES IN SCHEMA public TO reader; GRANT CREATE ON DATABASE app TO reader;",
    &["load", "reader"],
    "",
    (
        false,
        "Postgres query failed: must be owner of table items (SQLSTATE 42501)",
    ),
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
            description: "a snapshot stopped part-way resumes by copying the rest alone; a dropped slot needs --resnapshot",
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
            description: "tables that join a merge source are copied alone, even after a stop between them, while the other tables keep their history and soft deletes and still load their changes from before the copy",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &BACKFILL_MERGE,
            check_query: BACKFILL_MERGE_QUERY,
            expected_result: "1:10:9600:{a,b}:update:false,2:2:1:{c}:delete:true,3:11:1:-:update:false,4:4:1:{\"x y\",z}:insert:false | 1:one!:update,2:two!:update,3:three:insert | 1:b:update | 1:x:snapshot,2:y:insert",
        },
        CdcScenarioTestCase {
            description: "a table that joins a changelog source keeps none of its changes from before its copy",
            tag: "15-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &BACKFILL_CHANGELOG,
            check_query: "SELECT string_agg(_sqlload_op || ':' || id, ',' ORDER BY _sqlload_lsn, _sqlload_seq) FROM raw.log__extra",
            expected_result: "insert:9",
        },
        CdcScenarioTestCase {
            description: "a backfill stopped before its record or before streaming copies again and skips the changes its copy holds; a recorded table dropped from the publication is refused",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &BACKFILL_RESTARTS,
            check_query: "SELECT (SELECT string_agg(id || ':' || v || ':' || _sqlload_op, ',' ORDER BY id) FROM raw.app__extra) || ' | ' || (SELECT string_agg(id || ':' || v || ':' || _sqlload_op, ',' ORDER BY id) FROM raw.app__extra2)",
            expected_result: "1:b:snapshot,2:c:insert | 1:x:snapshot",
        },
        CdcScenarioTestCase {
            description: "a table that rejoins after --resnapshot left it out is refused before it is published, keeping the rows it held",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &BACKFILL_REJOIN,
            check_query: "SELECT string_agg(id || ':' || v || ':' || _sqlload_op, ',' ORDER BY id) FROM raw.app__extra",
            expected_result: "1:a:insert",
        },
        CdcScenarioTestCase {
            description: "a table that rejoins with an empty destination, or one holding only its own unfinished copy, is copied alone",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &BACKFILL_REJOIN_EMPTY,
            check_query: "SELECT string_agg(id || ':' || v || ':' || _sqlload_op, ',' ORDER BY id) FROM raw.app__extra",
            expected_result: "5:e:snapshot,6:g:update",
        },
        CdcScenarioTestCase {
            description: "a table missing from a named publication is refused with the fix, then copied alone once it is added by hand",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: NAMED_SOURCE,
            steps: &BACKFILL_NAMED,
            check_query: "SELECT string_agg(id || ':' || v || ':' || _sqlload_op, ',' ORDER BY id) FROM raw.named__extra",
            expected_result: "1:z:update,2:b:snapshot,3:c:snapshot",
        },
        CdcScenarioTestCase {
            description: "load --follow catches up, then writes what it read when --max-duration passes",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &FOLLOWED,
            check_query: MERGED_QUERY,
            expected_result: MERGED_RESULT,
        },
        CdcScenarioTestCase {
            description: "load --follow into a changelog holds every change once",
            tag: "15-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &FOLLOWED_LOG,
            check_query: CHANGELOG_QUERY,
            expected_result: CHANGELOG_RESULT,
        },
        CdcScenarioTestCase {
            description: "a follow stopped after its first table committed converges without duplicates",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &FOLLOW_AFTER_FIRST,
            check_query: MERGED_QUERY,
            expected_result: MERGED_RESULT,
        },
        CdcScenarioTestCase {
            description: "a changelog follow stopped before it confirmed converges without duplicates",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: "",
            steps: &FOLLOW_LOG_BEFORE_CONFIRM,
            check_query: CHANGELOG_QUERY,
            expected_result: CHANGELOG_RESULT,
        },
        CdcScenarioTestCase {
            description: "load --follow follows one postgres_cdc source and nothing else, and only under load",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: PLAIN_SOURCE,
            steps: &FOLLOW_REFUSED,
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.tables WHERE table_schema = 'raw'",
            expected_result: "0",
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
            description: "a generated column, and one outside the publication's column list, are refused before any snapshot, instead of never changing",
            tag: "15-alpine",
            spill_bytes: "",
            extra_config: UNSTREAMED_SOURCES,
            steps: &UNSTREAMED,
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM raw.gen__t",
            expected_result: "0",
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
            description: "a user that does not own the tables cannot create the publication, and the load says what Postgres said",
            tag: "17-alpine",
            spill_bytes: "",
            extra_config: NOT_OWNER_SOURCE,
            steps: &NOT_OWNER,
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM raw.reader__items",
            expected_result: "0",
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
            let project = cdc_project(postgres.port, &database)
                + &test_case
                    .extra_config
                    .replace("{port}", &postgres.port.to_string());
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

/// `load --follow` writes changes as they come, plans again when a table gains a column (once, even under --resnapshot), and stops cleanly at --max-duration or SIGTERM; a stop at the re-plan loses nothing.
#[test]
fn given_a_following_load_when_the_source_changes_then_each_change_lands_once_and_it_stops_cleanly()
-> Result<(), String> {
    let test_cases = [
        FollowTestCase {
            description: "changes written while following land, in batches, then it stops at --max-duration",
            tag: "17-alpine",
            args: FOLLOW_SHORT,
            crash_at: "",
            writes: &[FIRST_CHANGES, SECOND_CHANGES],
            batch_interval: "1s",
            signal: "-0",
            expected_advanced: true,
            expected_batches: 2,
            expected_follow: (true, "batch written"),
            expected_after: "0 rows",
            check_query: MERGED_QUERY,
            expected_result: MERGED_RESULT,
        },
        FollowTestCase {
            description: "Postgres 15: changes written while following land",
            tag: "15-alpine",
            args: FOLLOW_SHORT,
            crash_at: "",
            writes: &[FIRST_CHANGES, SECOND_CHANGES],
            batch_interval: "1s",
            signal: "-0",
            expected_advanced: true,
            expected_batches: 2,
            expected_follow: (true, "batch written"),
            expected_after: "0 rows",
            check_query: MERGED_QUERY,
            expected_result: MERGED_RESULT,
        },
        FollowTestCase {
            description: "a column added while following is planned, added and loaded without stopping",
            tag: "17-alpine",
            args: FOLLOW_SHORT,
            crash_at: "",
            writes: &[ADD_COLUMN],
            batch_interval: "1s",
            signal: "-0",
            expected_advanced: true,
            expected_batches: 1,
            expected_follow: (true, "re-planning gained=public.items gained column c\n"),
            expected_after: "0 rows",
            check_query: ITEMS_WITH_C,
            expected_result: "1:one!:-,2:two:8,3:three:7",
        },
        FollowTestCase {
            description: "--resnapshot recreates the slot once, not again at the re-plan",
            tag: "17-alpine",
            args: &[
                "load",
                "app",
                "--follow",
                "--max-duration",
                "15s",
                "--resnapshot",
                "--yes",
            ],
            crash_at: "",
            writes: &[ADD_COLUMN],
            batch_interval: "1s",
            signal: "-0",
            expected_advanced: true,
            expected_batches: 1,
            expected_follow: (true, "re-planning gained=public.items gained column c\n"),
            expected_after: "0 rows",
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM raw._sqlload_cdc_snapshots WHERE table_name = ''",
            expected_result: "2",
        },
        FollowTestCase {
            description: "a follow stopped as it plans again loses nothing; a catch-up load writes the batch",
            tag: "17-alpine",
            args: FOLLOW_SHORT,
            crash_at: "follow-before-replan",
            writes: &[ADD_COLUMN],
            batch_interval: "1s",
            signal: "-0",
            expected_advanced: false,
            expected_batches: 0,
            expected_follow: (false, ""),
            expected_after: "Completed",
            check_query: ITEMS_WITH_C,
            expected_result: "1:one!:-,2:two:8,3:three:7",
        },
        FollowTestCase {
            description: "SIGTERM writes what was read but not yet written, confirms it and stops",
            tag: "17-alpine",
            args: &["load", "app", "--follow"],
            crash_at: "",
            writes: &[CHANGES],
            batch_interval: "1m",
            signal: "-TERM",
            expected_advanced: false,
            expected_batches: 1,
            expected_follow: (true, "stopped following"),
            expected_after: "0 rows",
            check_query: MERGED_QUERY,
            expected_result: MERGED_RESULT,
        },
        FollowTestCase {
            description: "while idle it confirms the server's position past writes it does not publish",
            tag: "17-alpine",
            args: FOLLOW_SHORT,
            crash_at: "",
            writes: &[UNPUBLISHED_WRITES],
            batch_interval: "1s",
            signal: "-0",
            expected_advanced: true,
            expected_batches: 0,
            expected_follow: (true, "stopped following"),
            expected_after: "0 rows",
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM raw.app__orders",
            expected_result: "3",
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
            let project = cdc_project(postgres.port, &database).replacen(
                "include = [\"public.*\"]\n",
                &format!(
                    "include = [\"public.*\"]\nbatch_interval = \"{}\"\n",
                    test_case.batch_interval
                ),
                1,
            );
            std::fs::write(&config, project).map_err(|error| error.to_string())?;
            run_sqlload(&config, APP, ("", ""))?;
            let child = spawn_sqlload(&config, test_case.args, test_case.crash_at)?;
            client
                .batch_execute(AWAIT_STREAMING)
                .await
                .map_err(|error| error.to_string())?;
            for write in test_case.writes {
                tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
                client
                    .batch_execute(write)
                    .await
                    .map_err(|error| error.to_string())?;
            }
            let written: String = client
                .query_one("SELECT pg_current_wal_lsn()::text", &[])
                .await
                .map_err(|error| error.to_string())?
                .get(0);
            client
                .batch_execute(&AWAIT_ADVANCED.replace("{lsn}", &written))
                .await
                .map_err(|error| error.to_string())?;
            let advanced: bool = client
                .query_one(SLOT_ADVANCED, &[&written])
                .await
                .map_err(|error| error.to_string())?
                .get(0);
            let (success, output) = stop_sqlload(child, test_case.signal).await?;
            assert_eq!(
                advanced, test_case.expected_advanced,
                "{}: whether the slot confirmed the writes before the stop\n{output}",
                test_case.description
            );
            assert!(
                output.matches("batch written").count() >= test_case.expected_batches,
                "{}: fewer than {} batches written\n{output}",
                test_case.description,
                test_case.expected_batches
            );
            assert_eq!(
                success, test_case.expected_follow.0,
                "{}\n{output}",
                test_case.description
            );
            assert!(
                output.contains(test_case.expected_follow.1),
                "{}: output lacks {:?}\n{output}",
                test_case.description,
                test_case.expected_follow.1
            );
            assert!(
                !output.contains("run it again"),
                "{}: a follow, which plans again by itself, says to run it again\n{output}",
                test_case.description
            );
            let (_, after) = run_sqlload(&config, APP, ("", ""))?;
            assert!(
                after.contains(test_case.expected_after),
                "{}: the load after lacks {:?}\n{after}",
                test_case.description,
                test_case.expected_after
            );
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
