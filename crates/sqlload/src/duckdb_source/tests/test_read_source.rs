use std::collections::BTreeMap;

use arrow_array::{Array, Decimal128Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};

use crate::column_types::models::ColumnType;
use crate::duckdb_source::main::duckdb_reader::duckdb_reader;
use crate::duckdb_source::main::read_catalog::read_catalog;
use crate::duckdb_source::main::read_columns::read_columns;
use crate::duckdb_source::main::read_rows::read_rows;
use crate::duckdb_source::models::DuckDbReadRequest;
use crate::project::models::ConnectionConfig;

const SETUP: &str = "
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

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime")
}

#[test]
fn given_a_duckdb_file_when_reading_its_tables_and_a_query_then_rows_load_typed_and_the_file_is_left_as_it_was()
 {
    let folder = tempfile::tempdir().expect("a folder");
    let path = folder.path().join("shop.duckdb");
    duckdb::Connection::open(&path)
        .and_then(|connection| connection.execute_batch(SETUP))
        .expect("the database");
    let before = std::fs::read(&path).expect("the file");
    let reader = duckdb_reader(
        &ConnectionConfig::DuckDb {
            database: path.display().to_string(),
        },
        Some(256 << 20),
    )
    .expect("a reader");
    let queries = [(
        "recent".to_owned(),
        "SELECT id, amount FROM sales.orders WHERE id > 1; -- newest".to_owned(),
    )];
    let runtime = runtime();
    let tables = runtime
        .block_on(read_catalog(
            &reader,
            (&["sales.*".to_owned()], &["sales.tmp_*".to_owned()]),
            &queries,
        ))
        .expect("the catalog");
    let names: Vec<String> = tables.iter().map(|table| table.qualified_name()).collect();
    assert_eq!(names, ["sales.big_orders", "sales.orders", "recent"], "tables and views by include, less exclude, then queries");
    let orders = &tables[1];
    assert_eq!(orders.primary_key, ["id"]);
    assert_eq!(orders.comment.as_deref(), Some("Orders, one row each."));
    let total = orders.columns.iter().find(|column| column.name == "total").expect("total");
    assert!(total.column_type.is_err(), "HUGEINT must be pinned");
    assert!(read_columns(orders, &BTreeMap::new()).is_err(), "an unpinned HUGEINT is a problem");
    let pinned = BTreeMap::from([("total".to_owned(), ColumnType::Text)]);
    let columns = read_columns(orders, &pinned).expect("read columns");
    let mut batches: Vec<RecordBatch> = Vec::new();
    let rows = runtime
        .block_on(read_rows(
            &reader,
            &DuckDbReadRequest {
                table: orders,
                sql: None,
                columns: &columns,
                filter: Some("\"id\" <= 2"),
            },
            1,
            async |batch| {
                batches.push(batch);
                Ok(())
            },
        ))
        .expect("the rows");
    assert_eq!(rows, 2);
    assert_eq!(batches.len(), 2, "one batch per batch_rows");
    let batch = arrow_select::concat::concat_batches(&batches[0].schema(), &batches).expect("one batch");
    let column = |name: &str| batch.column_by_name(name).expect(name).clone();
    let ids = column("id");
    let ids = ids.as_any().downcast_ref::<Int64Array>().expect("int64 ids");
    assert_eq!(ids.values(), &[1, 2]);
    let amounts = column("amount");
    let amounts = amounts.as_any().downcast_ref::<Decimal128Array>().expect("decimals");
    assert_eq!((amounts.value(0), amounts.is_null(1), amounts.scale()), (1050, true, 2));
    let placed = column("placed_at");
    let placed = placed.as_any().downcast_ref::<TimestampMicrosecondArray>().expect("timestamps");
    assert_eq!(placed.value(1), 1_767_349_800_000_000, "2026-01-02 10:30 UTC");
    let text = |name: &str| {
        let array = column(name);
        let strings = array.as_any().downcast_ref::<StringArray>().expect(name);
        (0..strings.len()).map(|row| strings.is_valid(row).then(|| strings.value(row).to_owned())).collect::<Vec<_>>()
    };
    assert_eq!(text("name"), [Some("ann".to_owned()), Some("bo\"b".to_owned())]);
    assert_eq!(text("tags"), [Some(r#"["a","b"]"#.to_owned()), Some("[]".to_owned())]);
    assert_eq!(text("meta"), [Some(r#"{"rank":1}"#.to_owned()), Some(r#"{"rank":2}"#.to_owned())]);
    assert_eq!(text("total")[0].as_deref(), Some("170141183460469231731687303715884105727"));
    let recent = &tables[2];
    let columns = read_columns(recent, &BTreeMap::new()).expect("query columns");
    let rows = runtime
        .block_on(read_rows(
            &reader,
            &DuckDbReadRequest {
                table: recent,
                sql: Some(&queries[0].1),
                columns: &columns,
                filter: Some("\"id\" > 2"),
            },
            100,
            async |_| Ok(()),
        ))
        .expect("the query's rows");
    assert_eq!(rows, 1, "the filter applies outside the query, after its trailing comment");
    assert_eq!(std::fs::read(&path).expect("the file"), before, "the file is read, never written");
}

#[test]
fn given_a_memory_or_motherduck_database_when_making_a_reader_then_it_is_refused() {
    for database in [":memory:", "md:shop"] {
        let refused = duckdb_reader(&ConnectionConfig::DuckDb { database: database.to_owned() }, None);
        assert!(refused.is_err(), "{database}");
    }
}
