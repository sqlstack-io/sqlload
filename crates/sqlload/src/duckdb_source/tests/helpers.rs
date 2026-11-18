use crate::project::main::parse_project::parse_project;
use crate::project::models::ProjectConfig;

/// A shop database: a commented table with a primary key and a type that must be pinned, a view, and a scratch table.
pub(crate) const SHOP: &str = "
CREATE SCHEMA sales;
CREATE TABLE sales.orders (
    id INTEGER PRIMARY KEY,
    amount DECIMAL(12,2),
    name VARCHAR,
    placed_at TIMESTAMPTZ,
    tags VARCHAR[],
    meta STRUCT(rank INTEGER),
    total HUGEINT
);
COMMENT ON TABLE sales.orders IS 'Orders, one row each.';
INSERT INTO sales.orders VALUES
    (1, 10.50, 'ann', '2026-01-01 00:00:00+00', ['a', 'b'], {'rank': 1}, 170141183460469231731687303715884105727),
    (2, NULL, 'bo\"b', '2026-01-02 12:30:00+02', [], {'rank': 2}, 2),
    (3, 99999999.99, NULL, NULL, NULL, NULL, 3);
CREATE VIEW sales.big_orders AS SELECT id, amount FROM sales.orders WHERE amount > 50;
CREATE TABLE sales.tmp_scratch (x INTEGER);
";

/// A runtime for one test's reads.
pub(crate) fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime")
}

/// A project whose `shop` source reads `database` with `source_lines` in its table, and a `recent` query table.
pub(crate) fn shop_project(database: &str, source_lines: &str) -> Result<ProjectConfig, String> {
    parse_project(
        &format!(
            "[connections.shop]\nkind = \"duckdb\"\ndatabase = {database:?}\n[connections.wh]\nkind = \"duckdb\"\ndatabase = \":memory:\"\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"raw\"\n[sources.shop]\nkind = \"duckdb\"\nconnection = \"shop\"\ndescription = \"Shop.\"\nwrite_strategy = \"replace\"\n{source_lines}\n[sources.shop.tables.recent]\ndescription = \"Recent orders.\"\nsql = \"SELECT id, amount FROM sales.orders WHERE id > 1 -- newest\\n;\\n\"\n"
        ),
        &|_| None,
    )
    .map_err(|error| error.to_string())
}
