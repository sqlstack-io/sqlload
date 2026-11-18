use std::collections::BTreeMap;

use arrow_cast::display::array_value_to_string;
use arrow_select::concat::concat_batches;

use crate::column_types::models::ColumnType;
use crate::duckdb_source::main::duckdb_reader::duckdb_reader;
use crate::duckdb_source::main::read_catalog::read_catalog;
use crate::duckdb_source::main::read_columns::read_columns;
use crate::duckdb_source::main::read_rows::read_rows;
use crate::duckdb_source::models::DuckDbReadRequest;
use crate::duckdb_source::tests::helpers::{SHOP, runtime, shop_project};
use crate::duckdb_source::tests::test_types::{
    ReadCatalogTestCase, ReadRowsTestCase, RefusedReaderTestCase,
};

#[test]
fn given_a_duckdb_file_when_listing_its_catalog_then_include_and_exclude_pick_tables_and_views_before_query_tables()
-> Result<(), String> {
    let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
    let path = folder.path().join("shop.duckdb");
    duckdb::Connection::open(&path)
        .and_then(|connection| connection.execute_batch(SHOP))
        .map_err(|error| error.to_string())?;
    let test_cases = [
        ReadCatalogTestCase {
            description: "tables and views by include, less exclude, then the query table",
            source_lines: "include = [\"sales.*\"]\nexclude = [\"sales.tmp_*\"]",
            expected_tables: &["sales.big_orders", "sales.orders", "recent"],
        },
        ReadCatalogTestCase {
            description: "without include only the query table",
            source_lines: "",
            expected_tables: &["recent"],
        },
    ];
    for test_case in test_cases {
        let project = shop_project(&path.display().to_string(), test_case.source_lines)?;
        let source = &project.sources["shop"];
        let reader = duckdb_reader(&project, source).map_err(|error| error.to_string())?;
        let tables = runtime()
            .block_on(read_catalog(&reader, source))
            .map_err(|error| format!("{}: {error}", test_case.description))?;
        let names: Vec<String> = tables.iter().map(|table| table.qualified_name()).collect();
        assert_eq!(
            names, test_case.expected_tables,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_a_duckdb_file_when_reading_a_table_or_query_then_rows_load_typed_and_the_file_is_left_as_it_was()
-> Result<(), String> {
    let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
    let path = folder.path().join("shop.duckdb");
    duckdb::Connection::open(&path)
        .and_then(|connection| connection.execute_batch(SHOP))
        .map_err(|error| error.to_string())?;
    let before = std::fs::read(&path).map_err(|error| error.to_string())?;
    let project = shop_project(&path.display().to_string(), "include = [\"sales.orders\"]")?;
    let source = &project.sources["shop"];
    let reader = duckdb_reader(&project, source).map_err(|error| error.to_string())?;
    let runtime = runtime();
    let tables = runtime
        .block_on(read_catalog(&reader, source))
        .map_err(|error| error.to_string())?;
    let orders = &tables[0];
    assert_eq!(
        (orders.primary_key.as_slice(), orders.comment.as_deref()),
        (["id".to_owned()].as_slice(), Some("Orders, one row each.")),
        "the primary key and comment come from the catalog"
    );
    assert!(
        read_columns(orders, &BTreeMap::new()).is_err(),
        "an unpinned HUGEINT is a problem"
    );
    let pinned = BTreeMap::from([("total".to_owned(), ColumnType::Text)]);
    let test_cases = [
        ReadRowsTestCase {
            description: "the table, one row per batch, filtered: decimals, instants in UTC, lists and structs as JSON, a pinned HUGEINT as text",
            table: "sales.orders",
            filter: Some("\"id\" <= 2"),
            batch_rows: 1,
            expected_values: &[
                "id=1|2",
                "amount=10.50|",
                "name=ann|bo\"b",
                "placed_at=2026-01-01T00:00:00Z|2026-01-02T10:30:00Z",
                r#"tags=["a","b"]|[]"#,
                r#"meta={"rank":1}|{"rank":2}"#,
                "total=170141183460469231731687303715884105727|2",
            ],
        },
        ReadRowsTestCase {
            description: "the query table, its filter applied outside the query after its trailing comment",
            table: "recent",
            filter: Some("\"id\" > 2"),
            batch_rows: 100,
            expected_values: &["id=3", "amount=99999999.99"],
        },
    ];
    for test_case in test_cases {
        let table = tables
            .iter()
            .find(|table| table.qualified_name() == test_case.table)
            .ok_or(test_case.description)?;
        let columns = read_columns(table, &pinned).map_err(|problems| problems.join("; "))?;
        let mut batches = Vec::new();
        let request = DuckDbReadRequest {
            table,
            sql: source
                .tables
                .get(test_case.table)
                .and_then(|table| table.sql.as_deref()),
            columns: &columns,
            filter: test_case.filter,
        };
        runtime
            .block_on(read_rows(
                &reader,
                &request,
                test_case.batch_rows,
                async |batch| {
                    batches.push(batch);
                    Ok(())
                },
            ))
            .map_err(|error| format!("{}: {error}", test_case.description))?;
        let batch =
            concat_batches(&batches[0].schema(), &batches).map_err(|error| error.to_string())?;
        let values = test_case
            .expected_values
            .iter()
            .map(|expected| {
                let name = expected.split_once('=').map_or(*expected, |(name, _)| name);
                let column = batch
                    .column_by_name(name)
                    .ok_or_else(|| format!("no column {name}"))?;
                let values = (0..column.len())
                    .map(|row| array_value_to_string(column, row))
                    .collect::<Result<Vec<String>, _>>()
                    .map_err(|error| error.to_string())?;
                Ok(format!("{name}={}", values.join("|")))
            })
            .collect::<Result<Vec<String>, String>>()?;
        assert_eq!(
            values, test_case.expected_values,
            "{}",
            test_case.description
        );
    }
    assert_eq!(
        std::fs::read(&path).map_err(|error| error.to_string())?,
        before,
        "the file is read, never written"
    );
    Ok(())
}

#[test]
fn given_a_memory_or_motherduck_database_when_making_a_reader_then_it_is_refused()
-> Result<(), String> {
    let test_cases = [
        RefusedReaderTestCase {
            description: "an in-memory database holds nothing to read",
            database: ":memory:",
            expected_error: ":memory: holds no tables to read",
        },
        RefusedReaderTestCase {
            description: "a MotherDuck database is a motherduck source",
            database: "md:shop",
            expected_error: "kind = \"motherduck\"",
        },
    ];
    for test_case in test_cases {
        let project = shop_project(test_case.database, "")?;
        let refused = duckdb_reader(&project, &project.sources["shop"])
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(
            refused.contains(test_case.expected_error),
            "{}: {refused}",
            test_case.description
        );
    }
    Ok(())
}
