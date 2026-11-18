use crate::duckdb_helpers::duckdb_source_project;
use crate::helpers::{duckdb_execute, duckdb_text, run_sqlload};
use crate::test_types::DuckDbSourceTestCase;

const SOURCE: &str = "
CREATE SCHEMA sales;
CREATE TABLE sales.orders (
    id INTEGER PRIMARY KEY,
    amount DECIMAL(12,2),
    updated_at TIMESTAMP,
    tags VARCHAR[],
    card VARCHAR,
    region VARCHAR
);
INSERT INTO sales.orders VALUES
    (1, 10.50, '2026-01-01 00:00:00', ['a'], '4111', 'EU'),
    (2, 20.00, '2026-01-02 00:00:00', [], '4222', 'EU'),
    (3, 30.25, '2026-01-03 00:00:00', NULL, '4333', 'US');
CREATE VIEW sales.big_orders AS SELECT id, amount FROM sales.orders WHERE amount > 15;
CREATE TABLE sales.tmp_scratch (x INTEGER);
";

/// A second run's changes: order 2 updated, order 4 added, order 1 untouched.
const CHANGES: &str = "
UPDATE sales.orders SET amount = 21.00, updated_at = '2026-01-05 00:00:00' WHERE id = 2;
INSERT INTO sales.orders VALUES (4, 40.00, '2026-01-06 00:00:00', ['b', 'c'], '4444', 'EU');
";

/// Rows in id order with amount and the tags' JSON, the target's columns, and the view's and query table's rows.
const CHECK: &str = "SELECT (SELECT string_agg(id || ':' || amount || ':' || coalesce(CAST(tags AS VARCHAR), '-'), ',' ORDER BY id) FROM raw.shop__orders) || '|' || (SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'raw' AND table_name = 'shop__orders' AND column_name NOT LIKE '\\_sqlload%' ESCAPE '\\') || '|' || (SELECT count(*) FROM raw.shop__big_orders) || '|' || (SELECT string_agg(CAST(id AS VARCHAR), ',' ORDER BY id) FROM raw.shop__big_query)";

#[test]
fn given_a_duckdb_source_when_loading_again_then_the_cursor_merges_only_changes_and_sync_deletes_removes_gone_rows()
-> Result<(), String> {
    let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
    let (source, target) = (
        folder.path().join("shop.duckdb"),
        folder.path().join("out.duckdb"),
    );
    duckdb_execute(&source, SOURCE)?;
    let config = folder.path().join("sqlload.toml");
    std::fs::write(&config, duckdb_source_project(&source, &target))
        .map_err(|error| error.to_string())?;
    let (planned, plan) = run_sqlload(&config, &["plan"], ("", ""))?;
    assert!(planned, "{plan}");
    for table in [
        "raw.shop__orders",
        "raw.shop__big_orders",
        "raw.shop__big_query",
    ] {
        assert!(plan.contains(table), "plan lists {table}: {plan}");
    }
    assert!(
        !plan.contains("tmp_scratch"),
        "exclude drops the table: {plan}"
    );
    let test_cases = [
        DuckDbSourceTestCase {
            description: "the first load: only EU rows, card excluded, tags as JSON, the view and the query",
            source_change: "",
            arguments: &["load"],
            expected_rows: r#"1:10.50:["a"],2:20.00:[]|id,amount,updated_at,tags,region|2|3"#,
        },
        DuckDbSourceTestCase {
            description: "the second load merges the updated and new rows the cursor finds",
            source_change: CHANGES,
            arguments: &["load"],
            expected_rows: r#"1:10.50:["a"],2:21.00:[],4:40.00:["b","c"]|id,amount,updated_at,tags,region|3|3,4"#,
        },
        DuckDbSourceTestCase {
            description: "--sync-deletes removes the row the source no longer holds",
            source_change: "DELETE FROM sales.orders WHERE id = 1",
            arguments: &["load", "--sync-deletes"],
            expected_rows: r#"2:21.00:[],4:40.00:["b","c"]|id,amount,updated_at,tags,region|3|3,4"#,
        },
    ];
    for test_case in test_cases {
        duckdb_execute(&source, test_case.source_change)?;
        let (loaded, output) = run_sqlload(&config, test_case.arguments, ("", ""))?;
        assert!(loaded, "{}:\n{output}", test_case.description);
        assert_eq!(
            duckdb_text(&target, CHECK)?,
            test_case.expected_rows,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
