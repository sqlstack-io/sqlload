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

/// Set to a database file in the process [`write_from_another_process`] starts, which then tries to write that file.
const WRITE_FROM_ANOTHER_PROCESS: &str = "SQLLOAD_TEST_DUCKDB_WRITE_FILE";

/// The test that calls [`write_when_asked`] first, run again in a process of its own, says whether it could write the file at `path`.
pub(crate) fn write_from_another_process(path: &std::path::Path) -> Result<String, String> {
    let exe = std::env::current_exe().map_err(|error| error.to_string())?;
    let test = "duckdb_source::tests::read_source::given_open_reads_of_a_file_when_another_process_writes_it_then_it_is_locked_until_the_last_read_ends";
    let output = std::process::Command::new(exe)
        .args(["--exact", test, "--nocapture"])
        .env(WRITE_FROM_ANOTHER_PROCESS, path)
        .output()
        .map_err(|error| error.to_string())?;
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

/// In the process [`write_from_another_process`] starts: try to write the file it names, say whether that worked, and end the process.
pub(crate) fn write_when_asked() {
    let Some(path) = std::env::var_os(WRITE_FROM_ANOTHER_PROCESS) else {
        return;
    };
    let written = duckdb::Connection::open(path)
        .and_then(|connection| connection.execute_batch("CREATE TABLE written (x INTEGER)"));
    println!(
        "{}",
        written.map_or_else(
            |error| format!(
                "the file was {}: {error}",
                ["not opened", "locked"][usize::from(error.to_string().contains("lock"))]
            ),
            |()| "the file was written".to_owned()
        )
    );
    std::process::exit(0);
}
