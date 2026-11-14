//! Child tables from list fields (`unnest`), and dlt's naming.

use sqlload_testkit::clickhouse::constants::{
    PASSWORD as CLICKHOUSE_PASSWORD, USER as CLICKHOUSE_USER,
};
use sqlload_testkit::clickhouse::main::run_client::run_client;
use sqlload_testkit::clickhouse::main::start_clickhouse::start_clickhouse;
use sqlload_testkit::postgres::constants::PASSWORD;
use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use sqlload::adapters::contract::models::TableName;
use sqlload::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use sqlload::adapters::dispatch::main::session::execute::execute;
use sqlload::column_types::models::Warehouse;
use sqlload::loader::main::connect_targets::connect_targets;

use crate::helpers::{
    described_project, duckdb_execute, duckdb_text, load_ndjson_runs, run_async, run_sqlload,
};
use crate::test_types::{
    ClearRowsTestCase, FilesTargetUnnestTestCase, NamedLikeTestCase, RekeyedTestCase,
    RowIdTestCase, UnnestFailTestCase, UnnestPlanTestCase,
};

const COLUMNS: &str = "SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'raw' AND table_name = 'orders'";

const ORDERS: &str =
    "{\"id\": 1, \"items\": [{\"sku\": \"a\", \"parts\": [1, 2]}], \"tags\": [\"x\"]}\n";

/// The parent's columns once `unnest` takes its list fields.
const UNNESTED_COLUMNS: &str = "id,_sqlload_source_file,_sqlload_source_version,_sqlload_source_row,_sqlload_load_id,_sqlload_loaded_at,_sqlload_row_id";

/// The parent's columns under `unnest = "all"` when its rows also hold an object, which stays a column.
const ALL_COLUMNS: &str = "id,meta,_sqlload_source_file,_sqlload_source_version,_sqlload_source_row,_sqlload_load_id,_sqlload_loaded_at,_sqlload_row_id";

/// An append with `unnest = "all"`.
const ALL_SETTINGS: &str =
    "write_strategy = \"append\"\n[sources.orders.tables.orders]\nunnest = \"all\"\n";

/// Two runs under `unnest = "all"`: an object, a list of objects with a list in each, then a list field first seen in the second.
const ALL_RUNS: &[(&str, &str)] = &[
    (
        "a.ndjson",
        "{\"id\": 1, \"meta\": {\"k\": 1}, \"items\": [{\"sku\": \"a\", \"parts\": [1, 2]}]}\n",
    ),
    (
        "b.ndjson",
        "{\"id\": 2, \"meta\": {\"k\": 2}, \"items\": [{\"sku\": \"b\", \"parts\": []}], \"tags\": [\"t\"]}\n",
    ),
];

/// The settings of the replace with `unnest`.
const REPLACE_SETTINGS: &str = "write_strategy = \"replace\"\n[sources.orders.tables.orders]\nunnest = [\"items\", \"items.parts\", \"tags\"]\n";

/// Two runs of the replace: the second has no tags, and its item a new field.
const REPLACE_RUNS: &[(&str, &str)] = &[
    (
        "a.ndjson",
        "{\"id\": 1, \"items\": [{\"sku\": \"a\", \"parts\": [1, 2]}], \"tags\": [\"x\"]}\n",
    ),
    (
        "a.ndjson",
        "{\"id\": 3, \"items\": [{\"sku\": \"c\", \"qty\": 5, \"parts\": [7]}]}\n",
    ),
];

/// The replace's tags count, items count, and each order's items and parts joined by id and load.
const REPLACE_CHECK: &str = "SELECT (SELECT count(*) FROM raw.orders__tags) || '/' || (SELECT count(*) FROM raw.orders__items) || '/' || (SELECT string_agg(o.id || ':' || i.sku || ':' || i.qty || ':' || p.value, ',') FROM raw.orders o JOIN raw.orders__items i ON i._sqlload_parent_id = o._sqlload_row_id AND i._sqlload_load_id = o._sqlload_load_id JOIN raw.orders__items__parts p ON p._sqlload_parent_id = i._sqlload_row_id AND p._sqlload_load_id = o._sqlload_load_id)";

/// The settings of the append with `unnest`.
const APPEND_SETTINGS: &str = "write_strategy = \"append\"\n[sources.orders.tables.orders]\nunnest = [\"items\", \"items.parts\", \"tags\"]\n";

/// Two runs of the append: tags first appear in the second.
const APPEND_RUNS: &[(&str, &str)] = &[
    (
        "a.ndjson",
        "{\"id\": 1, \"items\": [{\"sku\": \"a\", \"parts\": [1, 2]}]}\n",
    ),
    (
        "b.ndjson",
        "{\"id\": 2, \"items\": [{\"sku\": \"b\", \"parts\": [3]}], \"tags\": [\"new\"]}\n",
    ),
];

/// The append's tags and parts joined to their orders by id and load, with each part's position.
const APPEND_CHECK: &str = "SELECT (SELECT string_agg(o.id || ':' || t.value, ',') FROM raw.orders o JOIN raw.orders__tags t ON t._sqlload_parent_id = o._sqlload_row_id AND t._sqlload_load_id = o._sqlload_load_id) || '/' || (SELECT string_agg(o.id || ':' || i.sku || ':' || p.value || '@' || p._sqlload_list_idx, ',' ORDER BY p.value) FROM raw.orders o JOIN raw.orders__items i ON i._sqlload_parent_id = o._sqlload_row_id AND i._sqlload_load_id = o._sqlload_load_id JOIN raw.orders__items__parts p ON p._sqlload_parent_id = i._sqlload_row_id AND p._sqlload_load_id = o._sqlload_load_id)";

/// A merge with `unnest`.
const MERGE_SETTINGS: &str = "write_strategy = \"merge\"\nunique_key = [\"id\"]\n[sources.orders.tables.orders]\nunnest = [\"items\", \"items.parts\"]\n";

/// Two runs of the merge: the second repeats order 1 in one file, so the row read last wins, and empties order 2's list.
const MERGE_RUNS: &[(&str, &str)] = &[
    (
        "a.ndjson",
        "{\"id\": 1, \"items\": [{\"sku\": \"a\", \"parts\": [1, 2]}, {\"sku\": \"b\", \"parts\": [3]}]}\n{\"id\": 2, \"items\": [{\"sku\": \"c\", \"parts\": [4]}]}\n",
    ),
    (
        "b.ndjson",
        "{\"id\": 1, \"items\": [{\"sku\": \"x\", \"parts\": [9]}]}\n{\"id\": 1, \"items\": [{\"sku\": \"d\", \"parts\": [5]}]}\n{\"id\": 2, \"items\": []}\n",
    ),
];

/// The merge's orders, items and parts counts, and each order's items and parts joined by id.
const MERGE_CHECK: &str = "SELECT (SELECT count(*) FROM raw.orders) || '/' || (SELECT count(*) FROM raw.orders__items) || '/' || (SELECT count(*) FROM raw.orders__items__parts) || '/' || (SELECT string_agg(o.id || ':' || i.sku || ':' || p.value, ',') FROM raw.orders o JOIN raw.orders__items i ON i._sqlload_parent_id = o._sqlload_row_id JOIN raw.orders__items__parts p ON p._sqlload_parent_id = i._sqlload_row_id)";

/// A merge's runs for roots with other ids: the first adds order 3, which the second leaves alone.
const REKEYED_RUNS: &[(&str, &str)] = &[
    (
        "a.ndjson",
        "{\"id\": 1, \"items\": [{\"sku\": \"a\", \"parts\": [1, 2]}, {\"sku\": \"b\", \"parts\": [3]}]}\n{\"id\": 2, \"items\": [{\"sku\": \"c\", \"parts\": [4]}]}\n{\"id\": 3, \"items\": [{\"sku\": \"e\", \"parts\": [6]}]}\n",
    ),
    (
        "b.ndjson",
        "{\"id\": 1, \"items\": [{\"sku\": \"x\", \"parts\": [9]}]}\n{\"id\": 1, \"items\": [{\"sku\": \"d\", \"parts\": [5]}]}\n{\"id\": 2, \"items\": []}\n",
    ),
];

/// Orders 1 and 3 given ids their keys don't derive, as dlt's `delete-insert` merge gives them random ones, their items and parts following; order 2 keeps its own.
const REKEY_SQL: &str = "UPDATE raw.orders SET _sqlload_row_id = 'old-' || _sqlload_row_id WHERE id <> 2; UPDATE raw.orders__items SET _sqlload_parent_id = 'old-' || _sqlload_parent_id, _sqlload_root_id = 'old-' || _sqlload_root_id WHERE 'old-' || _sqlload_root_id IN (SELECT _sqlload_row_id FROM raw.orders); UPDATE raw.orders__items__parts SET _sqlload_root_id = 'old-' || _sqlload_root_id WHERE 'old-' || _sqlload_root_id IN (SELECT _sqlload_row_id FROM raw.orders)";

/// The orders, items and parts counts after the rekeyed merge, and each order's items and parts joined by id, in order.
const REKEYED_CHECK: &str = "SELECT (SELECT count(*) FROM raw.orders) || '/' || (SELECT count(*) FROM raw.orders__items) || '/' || (SELECT count(*) FROM raw.orders__items__parts) || '/' || (SELECT string_agg(o.id || ':' || i.sku || ':' || p.value, ',' ORDER BY o.id) FROM raw.orders o JOIN raw.orders__items i ON i._sqlload_parent_id = o._sqlload_row_id JOIN raw.orders__items__parts p ON p._sqlload_parent_id = i._sqlload_row_id)";

/// [`MERGE_SETTINGS`] under dlt's naming.
const DLT_MERGE_SETTINGS: &str = "naming = \"dlt\"\nwrite_strategy = \"merge\"\nunique_key = [\"id\"]\n[sources.orders.tables.orders]\nunnest = [\"items\", \"items.parts\"]\n";

/// [`REKEY_SQL`] under dlt's naming.
const DLT_REKEY_SQL: &str = "UPDATE raw.orders SET _dlt_id = 'old-' || _dlt_id WHERE id <> 2; UPDATE raw.orders__items SET _dlt_parent_id = 'old-' || _dlt_parent_id, _dlt_root_id = 'old-' || _dlt_root_id WHERE 'old-' || _dlt_root_id IN (SELECT _dlt_id FROM raw.orders); UPDATE raw.orders__items__parts SET _dlt_root_id = 'old-' || _dlt_root_id WHERE 'old-' || _dlt_root_id IN (SELECT _dlt_id FROM raw.orders)";

/// [`REKEYED_CHECK`] under dlt's naming.
const DLT_REKEYED_CHECK: &str = "SELECT (SELECT count(*) FROM raw.orders) || '/' || (SELECT count(*) FROM raw.orders__items) || '/' || (SELECT count(*) FROM raw.orders__items__parts) || '/' || (SELECT string_agg(o.id || ':' || i.sku || ':' || p.value, ',' ORDER BY o.id) FROM raw.orders o JOIN raw.orders__items i ON i._dlt_parent_id = o._dlt_id JOIN raw.orders__items__parts p ON p._dlt_parent_id = i._dlt_id)";

/// The parent's columns on ClickHouse, in table order.
const CLICKHOUSE_COLUMNS: &str = "SELECT arrayStringConcat(groupArray(name), ',') FROM (SELECT name FROM system.columns WHERE database = 'raw' AND table = 'orders' ORDER BY position)";

/// [`REPLACE_CHECK`] in ClickHouse's SQL.
const CLICKHOUSE_REPLACE_CHECK: &str = "SELECT toString((SELECT count() FROM raw.orders__tags)) || '/' || toString((SELECT count() FROM raw.orders__items)) || '/' || (SELECT arrayStringConcat(arraySort(groupArray(toString(o.id) || ':' || i.sku || ':' || toString(i.qty) || ':' || toString(p.value))), ',') FROM raw.orders o JOIN raw.orders__items i ON i._sqlload_parent_id = o._sqlload_row_id AND i._sqlload_load_id = o._sqlload_load_id JOIN raw.orders__items__parts p ON p._sqlload_parent_id = i._sqlload_row_id AND p._sqlload_load_id = o._sqlload_load_id)";

/// [`APPEND_CHECK`] in ClickHouse's SQL.
const CLICKHOUSE_APPEND_CHECK: &str = "SELECT (SELECT arrayStringConcat(arraySort(groupArray(toString(o.id) || ':' || t.value)), ',') FROM raw.orders o JOIN raw.orders__tags t ON t._sqlload_parent_id = o._sqlload_row_id AND t._sqlload_load_id = o._sqlload_load_id) || '/' || (SELECT arrayStringConcat(arraySort(groupArray(toString(o.id) || ':' || i.sku || ':' || toString(p.value) || '@' || toString(p._sqlload_list_idx))), ',') FROM raw.orders o JOIN raw.orders__items i ON i._sqlload_parent_id = o._sqlload_row_id AND i._sqlload_load_id = o._sqlload_load_id JOIN raw.orders__items__parts p ON p._sqlload_parent_id = i._sqlload_row_id AND p._sqlload_load_id = o._sqlload_load_id)";

/// The staging table holding the merged roots in the clear test.
const CLEAR_STAGING: &str = "orders__sqlload_staging_20261113";

/// The parent's columns under [`DELETE_INSERT_SETTINGS`].
const DELETE_INSERT_COLUMNS: &str = "id,day,_sqlload_source_file,_sqlload_source_version,_sqlload_source_row,_sqlload_load_id,_sqlload_loaded_at,_sqlload_row_id";

/// A `delete_insert` on a date partition with `unnest`.
const DELETE_INSERT_SETTINGS: &str = "write_strategy = \"delete_insert\"\ncursor_column = \"day\"\npartition_columns = { day = \"date\" }\n[sources.orders.tables.orders]\nunnest = [\"items\", \"items.parts\"]\n";

/// Two days' files, then the second day's rewritten with other items.
const DELETE_INSERT_RUNS: &[(&str, &str)] = &[
    (
        "day=2026-11-03/a.ndjson",
        "{\"id\": 1, \"items\": [{\"sku\": \"a\", \"parts\": [1]}]}\n",
    ),
    (
        "day=2026-11-04/b.ndjson",
        "{\"id\": 2, \"items\": [{\"sku\": \"b\", \"parts\": [2, 3]}]}\n",
    ),
    (
        "day=2026-11-04/b.ndjson",
        "{\"id\": 2, \"items\": [{\"sku\": \"c\", \"parts\": [4]}]}\n",
    ),
];

/// Rows in orders/items/parts, then each part linked up to its order.
const DELETE_INSERT_CHECK: &str = "SELECT (SELECT count(*) FROM raw.orders) || '/' || (SELECT count(*) FROM raw.orders__items) || '/' || (SELECT count(*) FROM raw.orders__items__parts) || '/' || (SELECT string_agg(o.id || ':' || i.sku || ':' || p.value, ',' ORDER BY p.value) FROM raw.orders o JOIN raw.orders__items i ON i._sqlload_parent_id = o._sqlload_row_id JOIN raw.orders__items__parts p ON p._sqlload_parent_id = i._sqlload_row_id)";

/// [`DELETE_INSERT_CHECK`] in ClickHouse's SQL.
const CLICKHOUSE_DELETE_INSERT_CHECK: &str = "SELECT toString((SELECT count() FROM raw.orders)) || '/' || toString((SELECT count() FROM raw.orders__items)) || '/' || toString((SELECT count() FROM raw.orders__items__parts)) || '/' || (SELECT arrayStringConcat(arraySort(groupArray(toString(o.id) || ':' || i.sku || ':' || toString(p.value))), ',') FROM raw.orders o JOIN raw.orders__items i ON i._sqlload_parent_id = o._sqlload_row_id JOIN raw.orders__items__parts p ON p._sqlload_parent_id = i._sqlload_row_id)";

/// Tables seeded beside `orders` and named like its child tables: `orders__notes`, a plain table, and `orders__x__items`, the child table of another root `orders__x`; each with one row. `{parent_id}` is the naming's parent-id column.
const NAMED_LIKE_CHILDREN: &[&str] = &[
    "CREATE SCHEMA raw",
    "CREATE TABLE raw.orders__notes (id BIGINT, note TEXT)",
    "INSERT INTO raw.orders__notes VALUES (1, 'n')",
    "CREATE TABLE raw.orders__x (id BIGINT, row_id TEXT)",
    "INSERT INTO raw.orders__x VALUES (1, 'r')",
    "CREATE TABLE raw.orders__x__items (sku TEXT, {parent_id} TEXT)",
    "INSERT INTO raw.orders__x__items VALUES ('s', 'r')",
];

/// [`NAMED_LIKE_CHILDREN`] in ClickHouse's SQL.
const CLICKHOUSE_NAMED_LIKE_CHILDREN: &[&str] = &[
    "CREATE DATABASE raw",
    "CREATE TABLE raw.orders__notes (id Int64, note String) ENGINE = MergeTree ORDER BY tuple()",
    "INSERT INTO raw.orders__notes VALUES (1, 'n')",
    "CREATE TABLE raw.orders__x (id Int64, row_id String) ENGINE = MergeTree ORDER BY tuple()",
    "INSERT INTO raw.orders__x VALUES (1, 'r')",
    "CREATE TABLE raw.orders__x__items (sku String, {parent_id} String) ENGINE = MergeTree ORDER BY tuple()",
    "INSERT INTO raw.orders__x__items VALUES ('s', 'r')",
];

/// A replace under `unnest = "all"` whose second run has no tags, so the real child table `orders__tags` is emptied.
const NAMED_LIKE_RUNS: &[(&str, &str)] = &[
    (
        "a.ndjson",
        "{\"id\": 1, \"items\": [{\"sku\": \"a\"}], \"tags\": [\"x\"]}\n",
    ),
    ("a.ndjson", "{\"id\": 3, \"items\": [{\"sku\": \"c\"}]}\n"),
];

/// The rows of the seeded tables, then of the real child tables: `notes/x items/tags/items`.
const NAMED_LIKE_CHECK: &str = "SELECT (SELECT count(*) FROM raw.orders__notes) || '/' || (SELECT count(*) FROM raw.orders__x__items) || '/' || (SELECT count(*) FROM raw.orders__tags) || '/' || (SELECT count(*) FROM raw.orders__items)";

/// [`NAMED_LIKE_CHECK`] in ClickHouse's SQL.
const CLICKHOUSE_NAMED_LIKE_CHECK: &str = "SELECT toString((SELECT count() FROM raw.orders__notes)) || '/' || toString((SELECT count() FROM raw.orders__x__items)) || '/' || toString((SELECT count() FROM raw.orders__tags)) || '/' || toString((SELECT count() FROM raw.orders__items))";

/// Two runs into a files target: an order with two parts, then one with one.
const FILES_TARGET_RUNS: &[(&str, &str)] = &[
    (
        "a.ndjson",
        "{\"id\": 1, \"items\": [{\"sku\": \"a\", \"parts\": [1, 2]}]}\n",
    ),
    (
        "b.ndjson",
        "{\"id\": 2, \"items\": [{\"sku\": \"b\", \"parts\": [3]}]}\n",
    ),
];

/// The files target's order, item and part counts, and each order's items and parts joined by id.
const FILES_TARGET_CHECK: &str = "SELECT (SELECT count(*) FROM read_parquet('{out}/orders/**/*.parquet')) || '/' || (SELECT count(*) FROM read_parquet('{out}/orders__items/**/*.parquet')) || '/' || (SELECT count(*) FROM read_parquet('{out}/orders__items__parts/**/*.parquet')) || '/' || (SELECT coalesce(string_agg(o.id || ':' || i.sku || ':' || p.value, ',' ORDER BY p.value), '') FROM read_parquet('{out}/orders/**/*.parquet') o JOIN read_parquet('{out}/orders__items/**/*.parquet') i ON i._sqlload_parent_id = o._sqlload_row_id JOIN read_parquet('{out}/orders__items__parts/**/*.parquet') p ON p._sqlload_parent_id = i._sqlload_row_id)";

#[test]
fn given_nesting_settings_a_table_cannot_use_when_planning_then_plan_names_each_problem()
-> Result<(), String> {
    let test_cases = [
        UnnestPlanTestCase {
            description: "parent and child paths, dlt naming and its row id plan",
            settings: "write_strategy = \"replace\"\nnaming = \"dlt\"\nrow_id = true\n[sources.orders.tables.orders]\nunnest = [\"items\", \"items.parts\", \"tags\"]\n",
            files_target: false,
            file: ORDERS,
            expected_ok: true,
            expected_problem: "├── unnest  orders__items, orders__items__parts, orders__tags  child tables, columns inferred at load",
        },
        UnnestPlanTestCase {
            description: "unnest = \"all\" names the child tables of the list fields the sampled rows hold",
            settings: "write_strategy = \"replace\"\n[sources.orders.tables.orders]\nunnest = \"all\"\n",
            files_target: false,
            file: ORDERS,
            expected_ok: true,
            expected_problem: "├── unnest  every list field, 1000 levels down at most: orders__items, orders__tags and more as lists appear; columns inferred at load",
        },
        UnnestPlanTestCase {
            description: "a word other than all",
            settings: "write_strategy = \"replace\"\n[sources.orders.tables.orders]\nunnest = \"every\"\n",
            files_target: false,
            file: ORDERS,
            expected_ok: false,
            expected_problem: "unnest = \"every\": list the dotted paths of the list fields to unnest, or write \"all\"",
        },
        UnnestPlanTestCase {
            description: "a child path without its parent",
            settings: "write_strategy = \"replace\"\n[sources.orders.tables.orders]\nunnest = [\"items.parts\"]\n",
            files_target: false,
            file: ORDERS,
            expected_ok: false,
            expected_problem: "unnest \"items.parts\": its parent list \"items\" must be unnested too; add it",
        },
        UnnestPlanTestCase {
            description: "only the path deeper than max_depth",
            settings: "write_strategy = \"replace\"\n[sources.orders.tables.orders]\nunnest = [\"items\", \"items.parts\"]\nmax_depth = 1\n",
            files_target: false,
            file: ORDERS,
            expected_ok: false,
            expected_problem: "unnest \"items.parts\": it is 2 levels deep, past max_depth = 1; raise max_depth or drop it",
        },
        UnnestPlanTestCase {
            description: "an empty part",
            settings: "write_strategy = \"replace\"\n[sources.orders.tables.orders]\nunnest = [\"items\", \"items.\"]\n",
            files_target: false,
            file: ORDERS,
            expected_ok: false,
            expected_problem: "unnest \"items.\": a path names a field at each level, joined by \".\"",
        },
        UnnestPlanTestCase {
            description: "a masked list field",
            settings: "write_strategy = \"replace\"\n[sources.orders.tables.orders]\nunnest = [\"tags\"]\nmask = { tags = \"hash\" }\n",
            files_target: false,
            file: ORDERS,
            expected_ok: false,
            expected_problem: "unnest \"tags\": mask also names tags, which unnesting moves to child tables; drop one of them",
        },
        UnnestPlanTestCase {
            description: "a list field an exclude pattern matches",
            settings: "write_strategy = \"replace\"\n[sources.orders.tables.orders]\nunnest = [\"tags\"]\nexclude_columns = [\"ta*\"]\n",
            files_target: false,
            file: ORDERS,
            expected_ok: false,
            expected_problem: "unnest \"tags\": exclude_columns ta* also names tags",
        },
        UnnestPlanTestCase {
            description: "max_depth without unnest",
            settings: "write_strategy = \"replace\"\n[sources.orders.tables.orders]\nmax_depth = 2\n",
            files_target: false,
            file: ORDERS,
            expected_ok: false,
            expected_problem: "max_depth limits the child tables unnest makes, and this table sets no unnest",
        },
        UnnestPlanTestCase {
            description: "a files target with listed paths plans a folder per child table",
            settings: "write_strategy = \"replace\"\n[sources.orders.tables.orders]\nunnest = [\"items\", \"items.parts\"]\n",
            files_target: true,
            file: ORDERS,
            expected_ok: true,
            expected_problem: "├── unnest  orders__items, orders__items__parts  child tables, columns inferred at load",
        },
        UnnestPlanTestCase {
            description: "unnest = \"all\" on a files target, whose earlier child tables a load cannot find",
            settings: "write_strategy = \"replace\"\n[sources.orders.tables.orders]\nunnest = \"all\"\n",
            files_target: true,
            file: ORDERS,
            expected_ok: false,
            expected_problem: "unnest does not yet write child tables with unnest = \"all\" on a files target (list the paths)",
        },
        UnnestPlanTestCase {
            description: "a merge on a files target, which would rewrite the child tables' files",
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\n[sources.orders.tables.orders]\nunnest = [\"items\"]\n",
            files_target: true,
            file: ORDERS,
            expected_ok: false,
            expected_problem: "unnest does not yet write child tables with write_strategy = \"merge\" on a files target",
        },
        UnnestPlanTestCase {
            description: "the unnested field is not a column of the table: the plan lists id and items only",
            settings: "write_strategy = \"replace\"\n[sources.orders.tables.orders]\nunnest = [\"tags\"]\n",
            files_target: false,
            file: ORDERS,
            expected_ok: true,
            expected_problem: "├── id    int    -> BIGINT\n    ├── items json   -> JSON\n    └── + _sqlload_source_file",
        },
        UnnestPlanTestCase {
            description: "a merge plans its child tables, whose rows each commit replaces by root",
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\n[sources.orders.tables.orders]\nunnest = [\"tags\"]\n",
            files_target: false,
            file: ORDERS,
            expected_ok: true,
            expected_problem: "├── unnest  orders__tags  child tables, columns inferred at load",
        },
        UnnestPlanTestCase {
            description: "row ids turned off under dlt naming",
            settings: "write_strategy = \"replace\"\nnaming = \"dlt\"\nrow_id = false\n",
            files_target: false,
            file: ORDERS,
            expected_ok: false,
            expected_problem: "row_id = false: naming = \"dlt\" gives every row its _dlt_id",
        },
        UnnestPlanTestCase {
            description: "two columns loading as one name under dlt naming, both named",
            settings: "write_strategy = \"replace\"\nnaming = \"dlt\"\n",
            files_target: false,
            file: "{\"OrderID\": 1, \"Order ID\": 2}\n",
            expected_ok: false,
            expected_problem: "columns \"OrderID\" and \"Order ID\" both load as order_id under naming = \"dlt\"; exclude one with exclude_columns",
        },
        UnnestPlanTestCase {
            description: "under dlt naming a pin may name a sampled column by its loaded name",
            settings: "write_strategy = \"replace\"\nnaming = \"dlt\"\n[sources.orders.tables.orders]\ncolumns = { order_id = \"text\" }\n",
            files_target: false,
            file: "{\"OrderID\": 1}\n",
            expected_ok: true,
            expected_problem: "├── order_id text   -> VARCHAR",
        },
        UnnestPlanTestCase {
            description: "under SQLLoad's naming the same pin is a typo of the file's name",
            settings: "write_strategy = \"replace\"\n[sources.orders.tables.orders]\ncolumns = { order_id = \"text\" }\n",
            files_target: false,
            file: "{\"OrderID\": 1}\n",
            expected_ok: false,
            expected_problem: "did you mean \"OrderID\"?",
        },
        UnnestPlanTestCase {
            description: "under dlt naming an exclusion naming a flattened object is refused, naming its columns",
            settings: "write_strategy = \"replace\"\nnaming = \"dlt\"\nexclude_columns = [\"Customer\"]\n",
            files_target: false,
            file: "{\"id\": 1, \"Customer\": {\"Email\": \"e\", \"Name\": \"n\"}}\n",
            expected_ok: false,
            expected_problem: "exclude_columns Customer names Customer, which naming = \"dlt\" flattens into Customer__Email, Customer__Name; name those columns instead",
        },
        UnnestPlanTestCase {
            description: "under dlt naming a pin naming a flattened object by its loaded name is refused",
            settings: "write_strategy = \"replace\"\nnaming = \"dlt\"\n[sources.orders.tables.orders]\ncolumns = { customer = \"json\" }\n",
            files_target: false,
            file: "{\"id\": 1, \"Customer\": {\"Email\": \"e\"}}\n",
            expected_ok: false,
            expected_problem: "columns customer names Customer, which naming = \"dlt\" flattens into Customer__Email; name those columns instead",
        },
        UnnestPlanTestCase {
            description: "under dlt naming a mask naming an object NULL in some sampled rows is refused, as its keys would load unmasked",
            settings: "write_strategy = \"replace\"\nnaming = \"dlt\"\n[sources.orders.tables.orders]\nmask = { c = \"hash\" }\n",
            files_target: false,
            file: "{\"id\": 1, \"c\": null}\n{\"id\": 2, \"c\": {\"n\": \"s\"}}\n",
            expected_ok: false,
            expected_problem: "mask c names c, which naming = \"dlt\" flattens into c__n; name those columns instead",
        },
        UnnestPlanTestCase {
            description: "under dlt naming a key holding __ inside an object is no object of its own, so an exclusion naming it is no flattened object",
            settings: "write_strategy = \"replace\"\nnaming = \"dlt\"\nexclude_columns = [\"a__b\"]\n",
            files_target: false,
            file: "{\"id\": 1, \"a\": {\"b__c\": 1}}\n",
            expected_ok: true,
            expected_problem: "├── id",
        },
        UnnestPlanTestCase {
            description: "under dlt naming the same exclusion naming an object nested in an object is refused",
            settings: "write_strategy = \"replace\"\nnaming = \"dlt\"\nexclude_columns = [\"a__b\"]\n",
            files_target: false,
            file: "{\"id\": 1, \"a\": {\"b\": {\"c\": 1}}}\n",
            expected_ok: false,
            expected_problem: "exclude_columns a__b names a__b, which naming = \"dlt\" flattens into a__b__c; name those columns instead",
        },
        UnnestPlanTestCase {
            description: "under dlt naming an exclusion matching a flattened object's columns applies to them",
            settings: "write_strategy = \"replace\"\nnaming = \"dlt\"\nexclude_columns = [\"Customer*\"]\n",
            files_target: false,
            file: "{\"id\": 1, \"Customer\": {\"Email\": \"e\"}}\n",
            expected_ok: true,
            expected_problem: "├── id",
        },
        UnnestPlanTestCase {
            description: "excluding one of two colliding columns resolves it",
            settings: "write_strategy = \"replace\"\nnaming = \"dlt\"\nexclude_columns = [\"Order ID\"]\n",
            files_target: false,
            file: "{\"OrderID\": 1, \"Order ID\": 2}\n",
            expected_ok: true,
            expected_problem: "├── order_id int   -> BIGINT",
        },
    ];
    for test_case in &test_cases {
        let root = tempfile::tempdir().map_err(|error| error.to_string())?;
        std::fs::create_dir_all(root.path().join("drop")).map_err(|error| error.to_string())?;
        std::fs::write(root.path().join("drop/orders.ndjson"), test_case.file)
            .map_err(|error| error.to_string())?;
        let target = [
            format!(
                "[connections.wh]\nkind = \"duckdb\"\ndatabase = \"{}\"\n\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"raw\"\n",
                root.path().join("out.duckdb").display()
            ),
            "[targets.dev]\nconnection = \"files\"\npath = \"out/{table}/\"\n".to_owned(),
        ][usize::from(test_case.files_target)]
        .clone();
        let config = root.path().join("sqlload.toml");
        let text = format!(
            "{target}\n[connections.files]\nkind = \"filesystem\"\nroot = \"{}\"\n\n[sources.orders]\nkind = \"files\"\ndescription = \"Test table.\"\nconnection = \"files\"\npath = \"drop/\"\nformat = \"ndjson\"\n{}",
            root.path().display(),
            test_case.settings
        );
        std::fs::write(&config, text).map_err(|error| error.to_string())?;
        let (ok, output) = run_sqlload(&config, &["plan"], ("", ""))?;
        assert_eq!(
            (ok, output.contains(test_case.expected_problem)),
            (test_case.expected_ok, true),
            "{}: {output}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_unnest_when_loading_into_a_files_target_then_each_child_table_gets_its_own_linked_folder()
-> Result<(), String> {
    let test_cases = [
        FilesTargetUnnestTestCase {
            description: "an append: each run's orders, items and parts are added, linked by id",
            settings: "write_strategy = \"append\"\n[sources.orders.tables.orders]\nunnest = [\"items\", \"items.parts\"]\n",
            runs: FILES_TARGET_RUNS,
            check_query: FILES_TARGET_CHECK,
            expected_result: "2/2/3/1:a:1,1:a:2,2:b:3",
        },
        FilesTargetUnnestTestCase {
            description: "a replace: the child tables are replaced with their parent, so only the last run's rows stay",
            settings: "write_strategy = \"replace\"\n[sources.orders.tables.orders]\nunnest = [\"items\", \"items.parts\"]\n",
            runs: &[
                (
                    "a.ndjson",
                    "{\"id\": 1, \"items\": [{\"sku\": \"a\", \"parts\": [1, 2]}]}\n",
                ),
                (
                    "a.ndjson",
                    "{\"id\": 3, \"items\": [{\"sku\": \"c\", \"parts\": [7]}]}\n",
                ),
            ],
            check_query: FILES_TARGET_CHECK,
            expected_result: "1/1/1/3:c:7",
        },
        FilesTargetUnnestTestCase {
            description: "a replace whose rows hold no items any more: the child tables are emptied, not left with the last load's",
            settings: "write_strategy = \"replace\"\n[sources.orders.tables.orders]\nunnest = [\"items\", \"items.parts\"]\n",
            runs: &[
                (
                    "a.ndjson",
                    "{\"id\": 1, \"items\": [{\"sku\": \"a\", \"parts\": [1, 2]}]}\n",
                ),
                ("a.ndjson", "{\"id\": 3, \"items\": []}\n"),
            ],
            check_query: FILES_TARGET_CHECK,
            expected_result: "1/0/0/",
        },
    ];
    for test_case in &test_cases {
        let root = tempfile::tempdir().map_err(|error| error.to_string())?;
        std::fs::create_dir_all(root.path().join("drop")).map_err(|error| error.to_string())?;
        let config = root.path().join("sqlload.toml");
        let text = format!(
            "[targets.dev]\nconnection = \"files\"\npath = \"out/{{table}}/\"\n\n[connections.files]\nkind = \"filesystem\"\nroot = \"{}\"\n\n[sources.orders]\nkind = \"files\"\ndescription = \"Test table.\"\nconnection = \"files\"\npath = \"drop/\"\nformat = \"ndjson\"\n{}",
            root.path().display(),
            test_case.settings
        );
        std::fs::write(&config, text).map_err(|error| error.to_string())?;
        for (name, contents) in test_case.runs {
            std::fs::write(root.path().join("drop").join(name), contents)
                .map_err(|error| error.to_string())?;
            let (ok, output) = run_sqlload(&config, &["load"], ("", ""))?;
            assert!(ok, "{}: {output}", test_case.description);
        }
        let out = root.path().join("out").display().to_string();
        let result = duckdb_text(
            &root.path().join("check.duckdb"),
            &test_case.check_query.replace("{out}", &out),
        )?;
        assert_eq!(
            result, test_case.expected_result,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_row_id_or_unnest_settings_when_loading_then_rows_get_ids_and_lists_become_linked_child_tables()
-> Result<(), String> {
    let test_cases = [
        RowIdTestCase {
            description: "without row_id or naming the table is as before: no row id, SQLLoad's own names",
            settings: "write_strategy = \"replace\"\n",
            runs: &[("a.ndjson", "{\"id\": 1, \"v\": \"a\"}\n")],
            expected_columns: "id,v,_sqlload_source_file,_sqlload_source_version,_sqlload_source_row,_sqlload_load_id,_sqlload_loaded_at",
            check_query: "SELECT CAST(count(*) AS VARCHAR) FROM raw.orders",
            expected_result: "1",
        },
        RowIdTestCase {
            description: "row_id = true on an append: every row a new 14-character id, after the load time",
            settings: "row_id = true\nwrite_strategy = \"append\"\n",
            runs: &[
                (
                    "a.ndjson",
                    "{\"id\": 1, \"v\": \"a\"}\n{\"id\": 1, \"v\": \"a\"}\n",
                ),
                ("b.ndjson", "{\"id\": 1, \"v\": \"a\"}\n"),
            ],
            expected_columns: "id,v,_sqlload_source_file,_sqlload_source_version,_sqlload_source_row,_sqlload_load_id,_sqlload_loaded_at,_sqlload_row_id",
            check_query: "SELECT count(DISTINCT _sqlload_row_id) || '/' || count(*) || '/' || min(length(_sqlload_row_id)) || '/' || max(length(_sqlload_row_id)) FROM raw.orders",
            expected_result: "3/3/14/14",
        },
        RowIdTestCase {
            description: "naming = \"dlt\" on a merge: _dlt_load_id and _dlt_id, the id dlt's key hash of {\"id\":n}, kept when a later load changes the row",
            settings: "naming = \"dlt\"\nwrite_strategy = \"merge\"\nunique_key = [\"id\"]\n",
            runs: &[
                (
                    "a.ndjson",
                    "{\"id\": 1, \"v\": \"a\"}\n{\"id\": 2, \"v\": \"b\"}\n",
                ),
                ("b.ndjson", "{\"id\": 1, \"v\": \"c\"}\n"),
            ],
            expected_columns: "id,v,_sqlload_source_file,_sqlload_source_version,_sqlload_source_row,_dlt_load_id,_sqlload_loaded_at,_dlt_id",
            check_query: "SELECT string_agg(id || ':' || v || ':' || _dlt_id, ',' ORDER BY id) FROM raw.orders",
            expected_result: "1:c:z5jIxXIbUnmbKw,2:b:M6/KlLzJ2FeV/w",
        },
        RowIdTestCase {
            description: "naming = \"dlt\" names a file's columns as dlt does, merges by a key named as the file spells it, and unnests a list under its snake-cased name",
            settings: "naming = \"dlt\"\nwrite_strategy = \"merge\"\nunique_key = [\"OrderID\"]\n[sources.orders.tables.orders]\nunnest = [\"LineItems\"]\n",
            runs: &[
                (
                    "a.ndjson",
                    "{\"OrderID\": 1, \"Unit Price\": \"a\", \"a__B\": 1, \"LineItems\": [{\"UnitPrice\": 5}]}\n{\"OrderID\": 2, \"Unit Price\": \"b\", \"a__B\": 2, \"LineItems\": []}\n",
                ),
                (
                    "b.ndjson",
                    "{\"OrderID\": 1, \"Unit Price\": \"c\", \"a__B\": 3, \"LineItems\": [{\"UnitPrice\": 7}]}\n",
                ),
            ],
            expected_columns: "order_id,unit_price,a__b,_sqlload_source_file,_sqlload_source_version,_sqlload_source_row,_dlt_load_id,_sqlload_loaded_at,_dlt_id",
            check_query: "SELECT (SELECT string_agg(order_id || ':' || unit_price || ':' || a__b, ',' ORDER BY order_id) FROM raw.orders) || '/' || (SELECT string_agg(o.order_id || ':' || i.unit_price, ',') FROM raw.orders o JOIN raw.orders__line_items i ON i._dlt_parent_id = o._dlt_id)",
            expected_result: "1:c:3,2:b:2/1:7",
        },
        RowIdTestCase {
            description: "naming = \"dlt\" with a cursor on a partition column the naming renames: the second run reads only the new partition",
            settings: "naming = \"dlt\"\nwrite_strategy = \"append\"\ncursor_column = \"Day\"\npartition_columns = { Day = \"date\" }\n",
            runs: &[
                ("Day=2026-11-03/a.ndjson", "{\"ID\": 1}\n"),
                ("Day=2026-11-04/b.ndjson", "{\"ID\": 2}\n"),
            ],
            expected_columns: "id,day,_sqlload_source_file,_sqlload_source_version,_sqlload_source_row,_dlt_load_id,_sqlload_loaded_at,_dlt_id",
            check_query: "SELECT string_agg(id || ':' || day, ',' ORDER BY id) FROM raw.orders",
            expected_result: "1:2026-11-03,2:2026-11-04",
        },
        RowIdTestCase {
            description: "unnest on a replace: the lists leave the parent, the second run replaces parent and children alike, and a list no row has any more is emptied",
            settings: REPLACE_SETTINGS,
            runs: REPLACE_RUNS,
            expected_columns: UNNESTED_COLUMNS,
            check_query: REPLACE_CHECK,
            expected_result: "0/1/3:c:5:7",
        },
        RowIdTestCase {
            description: "unnest on an append: each run's children link to that run's rows by id and position, and a list first seen in run 2 gets its table then",
            settings: APPEND_SETTINGS,
            runs: APPEND_RUNS,
            expected_columns: UNNESTED_COLUMNS,
            check_query: APPEND_CHECK,
            expected_result: "2:new/1:a:1@0,1:a:2@1,2:b:3@0",
        },
        RowIdTestCase {
            description: "unnest on a merge: a merged order's items and parts are replaced by its new ones, a repeated key keeps only the kept row's, and an empty list leaves none",
            settings: MERGE_SETTINGS,
            runs: MERGE_RUNS,
            expected_columns: UNNESTED_COLUMNS,
            check_query: MERGE_CHECK,
            expected_result: "2/1/1/1:d:5",
        },
        RowIdTestCase {
            description: "unnest on a delete_insert: a rewritten partition's orders lose their old items and parts at every level, and another partition's keep theirs",
            settings: DELETE_INSERT_SETTINGS,
            runs: DELETE_INSERT_RUNS,
            expected_columns: DELETE_INSERT_COLUMNS,
            check_query: DELETE_INSERT_CHECK,
            expected_result: "2/2/2/1:a:1,2:c:4",
        },
        RowIdTestCase {
            description: "unnest under an enforced contract: columns declares the child tables' columns by dotted path; each table loads its declared columns and links, and leaves out the rest (an item's qty, the parent's note and tags)",
            settings: "write_strategy = \"replace\"\ncontract = \"enforced\"\n[sources.orders.tables.orders]\ncolumns = { id = \"int\", \"items.sku\" = \"text\", \"items.parts.value\" = \"int\" }\nunnest = [\"items\", \"items.parts\"]\n",
            runs: &[(
                "a.ndjson",
                "{\"id\": 1, \"note\": \"n\", \"items\": [{\"sku\": \"a\", \"qty\": 5, \"parts\": [1, 2]}], \"tags\": [\"x\"]}\n",
            )],
            expected_columns: UNNESTED_COLUMNS,
            check_query: "SELECT (SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'raw' AND table_name = 'orders__items') || '/' || (SELECT string_agg(o.id || ':' || i.sku || ':' || p.value, ',' ORDER BY p.value) FROM raw.orders o JOIN raw.orders__items i ON i._sqlload_parent_id = o._sqlload_row_id JOIN raw.orders__items__parts p ON p._sqlload_parent_id = i._sqlload_row_id)",
            expected_result: "sku,_sqlload_parent_id,_sqlload_list_idx,_sqlload_load_id,_sqlload_loaded_at,_sqlload_row_id/1:a:1,1:a:2",
        },
        RowIdTestCase {
            description: "unnest = \"all\" on an append: lists at every level become child tables, an object stays a column, and a list field first seen in run 2 gets its table then",
            settings: ALL_SETTINGS,
            runs: ALL_RUNS,
            expected_columns: ALL_COLUMNS,
            check_query: APPEND_CHECK,
            expected_result: "2:t/1:a:1@0,1:a:2@1",
        },
        RowIdTestCase {
            description: "unnest = \"all\" with on_schema_change = \"ignore\": the list field first seen in run 2 makes no table, and its items are left out",
            settings: "write_strategy = \"append\"\non_schema_change = \"ignore\"\n[sources.orders.tables.orders]\nunnest = \"all\"\n",
            runs: ALL_RUNS,
            expected_columns: ALL_COLUMNS,
            check_query: "SELECT (SELECT count(*) FROM information_schema.tables WHERE table_schema = 'raw' AND table_name = 'orders__tags') || '/' || (SELECT count(*) FROM raw.orders__items__parts)",
            expected_result: "0/2",
        },
    ];
    for test_case in &test_cases {
        let root = tempfile::tempdir().map_err(|error| error.to_string())?;
        let database = root.path().join("out.duckdb");
        let connection = format!("kind = \"duckdb\"\ndatabase = \"{}\"", database.display());
        load_ndjson_runs(
            root.path(),
            &connection,
            (test_case.settings, test_case.runs),
        )?;
        assert_eq!(
            [
                duckdb_text(&database, COLUMNS)?,
                duckdb_text(&database, test_case.check_query)?
            ],
            [test_case.expected_columns, test_case.expected_result],
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_roots_whose_ids_their_keys_did_not_give_when_merging_then_their_old_items_leave_too()
-> Result<(), String> {
    let test_cases = [
        RekeyedTestCase {
            description: "roots with other ids (as dlt's delete-insert gives them) beside one with its own: a merged order's old items and parts are replaced, an emptied list leaves none, and an order the merge leaves alone keeps its items",
            settings: MERGE_SETTINGS,
            loaded: &REKEYED_RUNS[..1],
            merged: &REKEYED_RUNS[1..],
            rekey: REKEY_SQL,
            check_query: REKEYED_CHECK,
            expected_result: "3/2/2/1:d:5,3:e:6",
        },
        RekeyedTestCase {
            description: "naming = \"dlt\" over tables dlt merged with delete-insert, whose _dlt_id is random: the merged orders' old items and parts go, the unmerged order's stay",
            settings: DLT_MERGE_SETTINGS,
            loaded: &REKEYED_RUNS[..1],
            merged: &REKEYED_RUNS[1..],
            rekey: DLT_REKEY_SQL,
            check_query: DLT_REKEYED_CHECK,
            expected_result: "3/2/2/1:d:5,3:e:6",
        },
    ];
    for test_case in &test_cases {
        let root = tempfile::tempdir().map_err(|error| error.to_string())?;
        let database = root.path().join("out.duckdb");
        let connection = format!("kind = \"duckdb\"\ndatabase = \"{}\"", database.display());
        load_ndjson_runs(
            root.path(),
            &connection,
            (test_case.settings, test_case.loaded),
        )?;
        duckdb_execute(&database, test_case.rekey)?;
        load_ndjson_runs(
            root.path(),
            &connection,
            (test_case.settings, test_case.merged),
        )?;
        assert_eq!(
            duckdb_text(&database, test_case.check_query)?,
            test_case.expected_result,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_roots_whose_ids_their_keys_did_not_give_when_merging_into_postgres_then_their_old_items_leave_too()
-> Result<(), String> {
    let test_cases = [
        RekeyedTestCase {
            description: "roots with other ids (as dlt's delete-insert gives them) beside one with its own: a merged order's old items and parts are replaced, an emptied list leaves none, and an order the merge leaves alone keeps its items",
            settings: MERGE_SETTINGS,
            loaded: &REKEYED_RUNS[..1],
            merged: &REKEYED_RUNS[1..],
            rekey: REKEY_SQL,
            check_query: REKEYED_CHECK,
            expected_result: "3/2/2/1:d:5,3:e:6",
        },
        RekeyedTestCase {
            description: "naming = \"dlt\" over tables dlt merged with delete-insert, whose _dlt_id is random: the merged orders' old items and parts go, the unmerged order's stay",
            settings: DLT_MERGE_SETTINGS,
            loaded: &REKEYED_RUNS[..1],
            merged: &REKEYED_RUNS[1..],
            rekey: DLT_REKEY_SQL,
            check_query: DLT_REKEYED_CHECK,
            expected_result: "3/2/2/1:d:5,3:e:6",
        },
    ];
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        let connection = format!(
            "kind = \"postgres\"\nhost = \"127.0.0.1\"\nport = {}\nuser = \"sqlload\"\npassword = \"{PASSWORD}\"\ndatabase = \"app\"",
            postgres.port
        );
        for test_case in &test_cases {
            client
                .batch_execute("DROP SCHEMA IF EXISTS raw CASCADE")
                .await
                .map_err(|error| error.to_string())?;
            let root = tempfile::tempdir().map_err(|error| error.to_string())?;
            load_ndjson_runs(
                root.path(),
                &connection,
                (test_case.settings, test_case.loaded),
            )?;
            client
                .batch_execute(test_case.rekey)
                .await
                .map_err(|error| format!("{}: {error}", test_case.rekey))?;
            load_ndjson_runs(
                root.path(),
                &connection,
                (test_case.settings, test_case.merged),
            )?;
            let row = client
                .query_one(test_case.check_query, &[])
                .await
                .map_err(|error| format!("{}: {error}", test_case.check_query))?;
            assert_eq!(
                row.get::<_, String>(0),
                test_case.expected_result,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_unnest_when_loading_into_postgres_then_lists_become_child_tables_linked_as_in_duckdb()
-> Result<(), String> {
    let test_cases = [
        RowIdTestCase {
            description: "unnest on a replace: the lists leave the parent, the second run replaces parent and children alike, and a list no row has any more is emptied (Postgres)",
            settings: REPLACE_SETTINGS,
            runs: REPLACE_RUNS,
            expected_columns: UNNESTED_COLUMNS,
            check_query: REPLACE_CHECK,
            expected_result: "0/1/3:c:5:7",
        },
        RowIdTestCase {
            description: "unnest on an append: each run's children link to that run's rows by id and position, and a list first seen in run 2 gets its table then (Postgres)",
            settings: APPEND_SETTINGS,
            runs: APPEND_RUNS,
            expected_columns: UNNESTED_COLUMNS,
            check_query: APPEND_CHECK,
            expected_result: "2:new/1:a:1@0,1:a:2@1,2:b:3@0",
        },
        RowIdTestCase {
            description: "unnest on a merge: a merged order's items and parts are replaced by its new ones, a repeated key keeps only the kept row's, and an empty list leaves none",
            settings: MERGE_SETTINGS,
            runs: MERGE_RUNS,
            expected_columns: UNNESTED_COLUMNS,
            check_query: MERGE_CHECK,
            expected_result: "2/1/1/1:d:5",
        },
        RowIdTestCase {
            description: "unnest on a delete_insert: a rewritten partition's orders lose their old items and parts at every level, and another partition's keep theirs (Postgres)",
            settings: DELETE_INSERT_SETTINGS,
            runs: DELETE_INSERT_RUNS,
            expected_columns: DELETE_INSERT_COLUMNS,
            check_query: DELETE_INSERT_CHECK,
            expected_result: "2/2/2/1:a:1,2:c:4",
        },
        RowIdTestCase {
            description: "unnest = \"all\" on an append: lists at every level become child tables, an object stays a column, and a list field first seen in run 2 gets its table then (Postgres)",
            settings: ALL_SETTINGS,
            runs: ALL_RUNS,
            expected_columns: ALL_COLUMNS,
            check_query: APPEND_CHECK,
            expected_result: "2:t/1:a:1@0,1:a:2@1",
        },
    ];
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        let connection = format!(
            "kind = \"postgres\"\nhost = \"127.0.0.1\"\nport = {}\nuser = \"sqlload\"\npassword = \"{PASSWORD}\"\ndatabase = \"app\"",
            postgres.port
        );
        for test_case in &test_cases {
            client
                .batch_execute("DROP SCHEMA IF EXISTS raw CASCADE")
                .await
                .map_err(|error| error.to_string())?;
            let root = tempfile::tempdir().map_err(|error| error.to_string())?;
            load_ndjson_runs(
                root.path(),
                &connection,
                (test_case.settings, test_case.runs),
            )?;
            let mut found: Vec<String> = Vec::new();
            for query in [COLUMNS, test_case.check_query] {
                let row = client
                    .query_one(query, &[])
                    .await
                    .map_err(|error| format!("{query}: {error}"))?;
                found.push(row.get(0));
            }
            assert_eq!(
                found,
                [test_case.expected_columns, test_case.expected_result],
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_unnest_when_loading_into_clickhouse_then_lists_become_child_tables_linked_as_in_duckdb()
-> Result<(), String> {
    let test_cases = [
        RowIdTestCase {
            description: "unnest on a replace: the lists leave the parent, the second run replaces parent and children alike, and a list no row has any more is emptied (ClickHouse)",
            settings: REPLACE_SETTINGS,
            runs: REPLACE_RUNS,
            expected_columns: UNNESTED_COLUMNS,
            check_query: CLICKHOUSE_REPLACE_CHECK,
            expected_result: "0/1/3:c:5:7",
        },
        RowIdTestCase {
            description: "unnest on an append: each run's children link to that run's rows by id and position, and a list first seen in run 2 gets its table then (ClickHouse)",
            settings: APPEND_SETTINGS,
            runs: APPEND_RUNS,
            expected_columns: UNNESTED_COLUMNS,
            check_query: CLICKHOUSE_APPEND_CHECK,
            expected_result: "2:new/1:a:1@0,1:a:2@1,2:b:3@0",
        },
        RowIdTestCase {
            description: "unnest on a delete_insert: a rewritten partition's orders lose their old items and parts at every level, and another partition's keep theirs (ClickHouse)",
            settings: DELETE_INSERT_SETTINGS,
            runs: DELETE_INSERT_RUNS,
            expected_columns: DELETE_INSERT_COLUMNS,
            check_query: CLICKHOUSE_DELETE_INSERT_CHECK,
            expected_result: "2/2/2/1:a:1,2:c:4",
        },
        RowIdTestCase {
            description: "unnest = \"all\" on an append: lists at every level become child tables, an object stays a column, and a list field first seen in run 2 gets its table then (ClickHouse)",
            settings: ALL_SETTINGS,
            runs: ALL_RUNS,
            expected_columns: ALL_COLUMNS,
            check_query: CLICKHOUSE_APPEND_CHECK,
            expected_result: "2:t/1:a:1@0,1:a:2@1",
        },
        RowIdTestCase {
            description: "naming = \"dlt\" on an append: the child table and its links take dlt's names (ClickHouse)",
            settings: "naming = \"dlt\"\nwrite_strategy = \"append\"\n[sources.orders.tables.orders]\nunnest = [\"LineItems\"]\n",
            runs: &[(
                "a.ndjson",
                "{\"OrderID\": 1, \"LineItems\": [{\"UnitPrice\": 5}, {\"UnitPrice\": 6}]}\n{\"OrderID\": 2, \"LineItems\": []}\n",
            )],
            expected_columns: "order_id,_sqlload_source_file,_sqlload_source_version,_sqlload_source_row,_dlt_load_id,_sqlload_loaded_at,_dlt_id",
            check_query: "SELECT arrayStringConcat(arraySort(groupArray(toString(o.order_id) || ':' || toString(i.unit_price) || '@' || toString(i._dlt_list_idx))), ',') FROM raw.orders o JOIN raw.orders__line_items i ON i._dlt_parent_id = o._dlt_id",
            expected_result: "1:5@0,1:6@1",
        },
        RowIdTestCase {
            description: "naming = \"dlt\" with a layout naming columns as the files spell them: bare names take the loaded names, an expression stays as written (ClickHouse)",
            settings: "naming = \"dlt\"\nwrite_strategy = \"append\"\n[sources.orders.tables.orders]\norder_by = [\"OrderID\", \"cityHash64(order_id)\"]\npartition_by = \"Region\"\n",
            runs: &[(
                "a.ndjson",
                "{\"OrderID\": 1, \"Region\": \"eu\"}\n{\"OrderID\": 2, \"Region\": \"us\"}\n",
            )],
            expected_columns: "order_id,region,_sqlload_source_file,_sqlload_source_version,_sqlload_source_row,_dlt_load_id,_sqlload_loaded_at,_dlt_id",
            check_query: "SELECT sorting_key || '/' || partition_key || '/' || toString((SELECT count() FROM raw.orders)) FROM system.tables WHERE database = 'raw' AND name = 'orders'",
            expected_result: "order_id, cityHash64(order_id)/region/2",
        },
    ];
    run_async(async {
        let clickhouse = start_clickhouse().await?;
        let connection = format!(
            "kind = \"clickhouse\"\nhost = \"127.0.0.1\"\nport = {}\nuser = \"{CLICKHOUSE_USER}\"\npassword = \"{CLICKHOUSE_PASSWORD}\"",
            clickhouse.port
        );
        for test_case in &test_cases {
            run_client(&clickhouse, "DROP DATABASE IF EXISTS raw", &[]).await?;
            let root = tempfile::tempdir().map_err(|error| error.to_string())?;
            load_ndjson_runs(
                root.path(),
                &connection,
                (test_case.settings, test_case.runs),
            )?;
            let mut found: Vec<String> = Vec::new();
            for query in [CLICKHOUSE_COLUMNS, test_case.check_query] {
                found.push(run_client(&clickhouse, query, &[]).await?.trim().to_owned());
            }
            assert_eq!(
                found,
                [test_case.expected_columns, test_case.expected_result],
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_merged_roots_when_clearing_child_tables_loaded_to_clickhouse_then_only_their_items_leave()
-> Result<(), String> {
    let test_cases = [
        ClearRowsTestCase {
            description: "an unpartitioned table loses the merged roots' items; another root's and a NULL root's stay",
            partition_by: "",
            rows: "('r1', 'a'), ('r1', 'b'), ('r2', 'c'), (NULL, 'n')",
            roots: "('r1'), ('r3')",
            expected_rows: "NULL:n,r2:c",
            expected_staged: "r3",
        },
        ClearRowsTestCase {
            description: "a table partitioned by sku: every partition holding a merged root's items is rebuilt, and one left empty goes",
            partition_by: "PARTITION BY sku",
            rows: "('r1', 'a'), ('r2', 'a'), ('r1', 'b'), ('r3', 'c')",
            roots: "('r1')",
            expected_rows: "r2:a,r3:c",
            expected_staged: "",
        },
        ClearRowsTestCase {
            description: "a single partition emptied by the clear goes",
            partition_by: "PARTITION BY sku",
            rows: "('r1', 'a'), ('r2', 'b')",
            roots: "('r1')",
            expected_rows: "r2:b",
            expected_staged: "",
        },
        ClearRowsTestCase {
            description: "roots with no items leave the table as it was",
            partition_by: "PARTITION BY sku",
            rows: "('r1', 'a'), ('r2', 'b')",
            roots: "('r9')",
            expected_rows: "r1:a,r2:b",
            expected_staged: "r9",
        },
    ];
    run_async(async {
        let clickhouse = start_clickhouse().await?;
        let project = described_project(&format!(
            "[connections.wh]\nkind = \"clickhouse\"\nhost = \"127.0.0.1\"\nport = {}\nuser = \"{CLICKHOUSE_USER}\"\npassword = \"{CLICKHOUSE_PASSWORD}\"\n\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"raw\"\n",
            clickhouse.port
        ))
        .await?;
        let destinations = connect_targets(&project)
            .await
            .map_err(|error| error.to_string())?;
        let serving = destinations
            .get("dev")
            .ok_or_else(|| "no ClickHouse destination".to_owned())?;
        let adapter = sql_adapter(Warehouse::ClickHouse);
        let table = TableName {
            database: None,
            schema: "raw".to_owned(),
            table: "orders__items".to_owned(),
        };
        let staging = TableName {
            table: CLEAR_STAGING.to_owned(),
            ..table.clone()
        };
        let condition = format!(
            "{} IN (SELECT {} FROM {})",
            adapter.render_identifier("_sqlload_root_id"),
            adapter.render_identifier("_sqlload_row_id"),
            adapter.render_qualified_name(&staging)
        );
        let scratch = TableName {
            table: format!("{CLEAR_STAGING}_0"),
            ..table.clone()
        };
        for test_case in &test_cases {
            for setup in [
                "DROP DATABASE IF EXISTS raw".to_owned(),
                "CREATE DATABASE raw".to_owned(),
                format!(
                    "CREATE TABLE raw.orders__items (_sqlload_root_id Nullable(String), sku String) ENGINE = MergeTree ORDER BY tuple() {}",
                    test_case.partition_by
                ),
                format!("INSERT INTO raw.orders__items VALUES {}", test_case.rows),
                format!(
                    "CREATE TABLE raw.{CLEAR_STAGING} (_sqlload_row_id String) ENGINE = MergeTree ORDER BY tuple()"
                ),
                format!("INSERT INTO raw.{CLEAR_STAGING} VALUES {}", test_case.roots),
            ] {
                run_client(&clickhouse, &setup, &[]).await?;
            }
            execute(
                serving,
                adapter.render_clear_rows(&table, &condition, &scratch),
            )
            .await
            .map_err(|error| error.to_string())?;
            execute(
                serving,
                vec![adapter.render_delete_staged(&staging, "_sqlload_row_id IN ('r1')")],
            )
            .await
            .map_err(|error| error.to_string())?;
            let mut found: Vec<String> = Vec::new();
            for query in [
                "SELECT arrayStringConcat(arraySort(groupArray(ifNull(_sqlload_root_id, 'NULL') || ':' || sku)), ',') FROM raw.orders__items",
                "SELECT arrayStringConcat(arraySort(groupArray(name)), ',') FROM system.tables WHERE database = 'raw'",
                &format!(
                    "SELECT arrayStringConcat(arraySort(groupArray(_sqlload_row_id)), ',') FROM raw.{CLEAR_STAGING}"
                ),
            ] {
                found.push(run_client(&clickhouse, query, &[]).await?.trim().to_owned());
            }
            assert_eq!(
                found,
                [
                    test_case.expected_rows.to_owned(),
                    format!("orders__items,{CLEAR_STAGING}"),
                    test_case.expected_staged.to_owned()
                ],
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_unnest_when_a_later_load_breaks_what_it_chose_then_the_load_fails_naming_the_field()
-> Result<(), String> {
    let test_cases = [
        UnnestFailTestCase {
            description: "an object where unnest = \"all\" took a list field fails, naming the file and the field",
            settings: ALL_SETTINGS,
            runs: &[("a.ndjson", "{\"id\": 1, \"tags\": [\"x\"]}\n")],
            failing: ("b.ndjson", "{\"id\": 2, \"tags\": {\"k\": \"y\"}}\n"),
            expected_error: "b.ndjson: tags: unnest = \"all\" took tags as a list field from its values so far, and a row holds a value that is not a list there",
            expected_after: ("SELECT CAST(count(*) AS VARCHAR) FROM raw.orders", "1"),
        },
        UnnestFailTestCase {
            description: "a nested list an earlier load took, whose child table exists, fails on a later value that is not a list",
            settings: ALL_SETTINGS,
            runs: &[("a.ndjson", "{\"id\": 1, \"items\": [{\"parts\": [1]}]}\n")],
            failing: ("b.ndjson", "{\"id\": 2, \"items\": [{\"parts\": \"x\"}]}\n"),
            expected_error: "items.parts: unnest = \"all\" took items.parts as a list field from its values so far",
            expected_after: ("SELECT CAST(count(*) AS VARCHAR) FROM raw.orders", "1"),
        },
        UnnestFailTestCase {
            description: "under dlt naming a list inside an object inside items keeps its existing child table's choice",
            settings: "naming = \"dlt\"\nwrite_strategy = \"append\"\n[sources.orders.tables.orders]\nunnest = \"all\"\n",
            runs: &[(
                "a.ndjson",
                "{\"id\": 1, \"items\": [{\"dims\": {\"tags\": [\"t\"]}}]}\n",
            )],
            failing: (
                "b.ndjson",
                "{\"id\": 2, \"items\": [{\"dims\": {\"tags\": \"x\"}}]}\n",
            ),
            expected_error: "items.dims__tags: unnest = \"all\" took items.dims__tags as a list field from its values so far",
            expected_after: ("SELECT CAST(count(*) AS VARCHAR) FROM raw.orders", "1"),
        },
        UnnestFailTestCase {
            description: "a list field first seen in a later load under on_schema_change = \"fail\" is refused, naming its table",
            settings: "write_strategy = \"append\"\non_schema_change = \"fail\"\n[sources.orders.tables.orders]\nunnest = \"all\"\n",
            runs: &[("a.ndjson", "{\"id\": 1, \"items\": [{\"sku\": \"a\"}]}\n")],
            failing: (
                "b.ndjson",
                "{\"id\": 2, \"items\": [{\"sku\": \"b\"}], \"tags\": [\"t\"]}\n",
            ),
            expected_error: "new list field tags would make the child table orders__tags and on_schema_change = \"fail\"",
            expected_after: ("SELECT CAST(count(*) AS VARCHAR) FROM raw.orders", "1"),
        },
        UnnestFailTestCase {
            description: "under dlt naming a later file's column loading as an existing one's name fails, naming both",
            settings: "naming = \"dlt\"\nwrite_strategy = \"append\"\n",
            runs: &[("a.ndjson", "{\"OrderID\": 1}\n")],
            failing: ("b.ndjson", "{\"OrderID\": 2, \"Order ID\": 3}\n"),
            expected_error: "columns \"OrderID\" and \"Order ID\" both load as order_id",
            expected_after: ("SELECT CAST(count(*) AS VARCHAR) FROM raw.orders", "1"),
        },
        UnnestFailTestCase {
            description: "under dlt naming two keys of one child item loading as one name fail, naming both",
            settings: "naming = \"dlt\"\nwrite_strategy = \"append\"\n[sources.orders.tables.orders]\nunnest = [\"items\"]\n",
            runs: &[("a.ndjson", "{\"id\": 1, \"items\": [{\"UnitPrice\": 1}]}\n")],
            failing: (
                "b.ndjson",
                "{\"id\": 2, \"items\": [{\"UnitPrice\": 1, \"unit price\": 2}]}\n",
            ),
            expected_error: "items: keys \"UnitPrice\" and \"unit price\" both load as unit_price",
            expected_after: ("SELECT CAST(count(*) AS VARCHAR) FROM raw.orders", "1"),
        },
        UnnestFailTestCase {
            description: "a merge its parent's checks refuse writes no child rows: the roots keep their items",
            settings: "write_strategy = \"merge\"\nunique_key = [\"id\"]\n[sources.orders.tables.orders]\nunnest = [\"items\"]\nchecks = { not_null = [\"v\"] }\n",
            runs: &[(
                "a.ndjson",
                "{\"id\": 1, \"v\": \"a\", \"items\": [{\"sku\": \"x\"}]}\n",
            )],
            failing: (
                "b.ndjson",
                "{\"id\": 1, \"v\": null, \"items\": [{\"sku\": \"y\"}, {\"sku\": \"z\"}]}\n",
            ),
            expected_error: "not_null",
            expected_after: (
                "SELECT string_agg(sku, ',' ORDER BY sku) FROM raw.orders__items",
                "x",
            ),
        },
        UnnestFailTestCase {
            description: "a replace its parent's min_rows refuses keeps the child tables it would have replaced",
            settings: "write_strategy = \"replace\"\n[sources.orders.tables.orders]\nunnest = [\"items\"]\nchecks = { min_rows = 2 }\n",
            runs: &[(
                "a.ndjson",
                "{\"id\": 1, \"items\": [{\"sku\": \"x\"}]}\n{\"id\": 2, \"items\": []}\n",
            )],
            failing: ("a.ndjson", "{\"id\": 3, \"items\": [{\"sku\": \"y\"}]}\n"),
            expected_error: "min_rows",
            expected_after: (
                "SELECT string_agg(sku, ',' ORDER BY sku) FROM raw.orders__items",
                "x",
            ),
        },
    ];
    for test_case in &test_cases {
        let root = tempfile::tempdir().map_err(|error| error.to_string())?;
        let connection = format!(
            "kind = \"duckdb\"\ndatabase = \"{}\"",
            root.path().join("out.duckdb").display()
        );
        let config = load_ndjson_runs(
            root.path(),
            &connection,
            (test_case.settings, test_case.runs),
        )?;
        let (name, contents) = test_case.failing;
        std::fs::write(root.path().join("drop").join(name), contents)
            .map_err(|error| error.to_string())?;
        let (ok, output) = run_sqlload(&config, &["load"], ("", ""))?;
        assert_eq!(
            (ok, output.contains(test_case.expected_error)),
            (false, true),
            "{}: {output}",
            test_case.description
        );
        let (query, expected) = test_case.expected_after;
        assert_eq!(
            duckdb_text(&root.path().join("out.duckdb"), query)?,
            expected,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_tables_named_like_child_tables_when_unnest_all_replaces_then_only_its_own_child_tables_are_emptied()
-> Result<(), String> {
    let test_cases = [
        NamedLikeTestCase {
            description: "a plain table and another root's child table named like child tables keep their rows, and a real child table no row fills is emptied (naming = \"sqlload\")",
            settings: "write_strategy = \"replace\"\n[sources.orders.tables.orders]\nunnest = \"all\"\n",
            parent_id: "_sqlload_parent_id",
            expected_result: "1/1/0/1",
        },
        NamedLikeTestCase {
            description: "a plain table and another root's child table named like child tables keep their rows, and a real child table no row fills is emptied (naming = \"dlt\", which names the parent-id column _dlt_parent_id)",
            settings: "write_strategy = \"replace\"\nnaming = \"dlt\"\n[sources.orders.tables.orders]\nunnest = \"all\"\n",
            parent_id: "_dlt_parent_id",
            expected_result: "1/1/0/1",
        },
    ];
    let seed = |statements: &[&str], parent_id: &str| -> Vec<String> {
        statements
            .iter()
            .map(|statement| statement.replace("{parent_id}", parent_id))
            .collect()
    };
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        let postgres_connection = format!(
            "kind = \"postgres\"\nhost = \"127.0.0.1\"\nport = {}\nuser = \"sqlload\"\npassword = \"{PASSWORD}\"\ndatabase = \"app\"",
            postgres.port
        );
        let clickhouse = start_clickhouse().await?;
        let clickhouse_connection = format!(
            "kind = \"clickhouse\"\nhost = \"127.0.0.1\"\nport = {}\nuser = \"{CLICKHOUSE_USER}\"\npassword = \"{CLICKHOUSE_PASSWORD}\"",
            clickhouse.port
        );
        for test_case in &test_cases {
            let root = tempfile::tempdir().map_err(|error| error.to_string())?;
            let database = root.path().join("out.duckdb");
            duckdb_execute(
                &database,
                &seed(NAMED_LIKE_CHILDREN, test_case.parent_id).join(";\n"),
            )?;
            load_ndjson_runs(
                &root.path().join("duckdb"),
                &format!("kind = \"duckdb\"\ndatabase = \"{}\"", database.display()),
                (test_case.settings, NAMED_LIKE_RUNS),
            )?;
            client
                .batch_execute("DROP SCHEMA IF EXISTS raw CASCADE")
                .await
                .map_err(|error| error.to_string())?;
            for statement in seed(NAMED_LIKE_CHILDREN, test_case.parent_id) {
                client
                    .batch_execute(&statement)
                    .await
                    .map_err(|error| format!("{statement}: {error}"))?;
            }
            load_ndjson_runs(
                &root.path().join("postgres"),
                &postgres_connection,
                (test_case.settings, NAMED_LIKE_RUNS),
            )?;
            run_client(&clickhouse, "DROP DATABASE IF EXISTS raw", &[]).await?;
            for statement in seed(CLICKHOUSE_NAMED_LIKE_CHILDREN, test_case.parent_id) {
                run_client(&clickhouse, &statement, &[]).await?;
            }
            load_ndjson_runs(
                &root.path().join("clickhouse"),
                &clickhouse_connection,
                (test_case.settings, NAMED_LIKE_RUNS),
            )?;
            let postgres_found: String = client
                .query_one(NAMED_LIKE_CHECK, &[])
                .await
                .map_err(|error| format!("{NAMED_LIKE_CHECK}: {error}"))?
                .get(0);
            assert_eq!(
                [
                    duckdb_text(&database, NAMED_LIKE_CHECK)?,
                    postgres_found,
                    run_client(&clickhouse, CLICKHOUSE_NAMED_LIKE_CHECK, &[])
                        .await?
                        .trim()
                        .to_owned(),
                ],
                [test_case.expected_result; 3],
                "{} (DuckDB, Postgres, ClickHouse)",
                test_case.description
            );
        }
        Ok(())
    })
}
