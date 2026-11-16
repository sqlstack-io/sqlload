use sqlload_testkit::fake_databricks::constants::FAKE_CATALOG;
use sqlload_testkit::fake_databricks::main::start_fake_databricks::start_fake_databricks;
use sqlload_testkit::fake_databricks::models::FakeDatabricks;

use crate::bigquery_helpers::{first_orders, later_orders};
use crate::helpers::{duckdb_text, run_async, run_sqlload};
use crate::test_types::DatabricksSourceTestCase;

const ORDERS: &str = "SELECT string_agg(CAST(id AS VARCHAR) || ':' || label || ':' || CAST(amount AS VARCHAR) || ':' || CAST(\"at\" AS VARCHAR), ',' ORDER BY id) FROM raw.marts__orders";

/// A project reading `marts.*` of the fake's catalog into a DuckDB file; the connection has no volume, which only a target needs.
fn source_project(fake: &FakeDatabricks, database: &std::path::Path) -> String {
    format!(
        "[connections.dbx]\nkind = \"databricks\"\nhost = \"{}\"\nclient_id = \"id\"\nclient_secret = \"secret\"\nwarehouse_id = \"wh\"\ncatalog = \"{FAKE_CATALOG}\"\n\n[connections.local]\nkind = \"duckdb\"\ndatabase = \"{}\"\n\n[targets.dev]\nconnection = \"local\"\nloader_schema = \"raw\"\n\n[sources.marts]\nkind = \"databricks\"\nconnection = \"dbx\"\ninclude = [\"marts.*\"]\n\n[sources.marts.tables.\"marts.orders\"]\ndescription = \"Orders.\"\ncursor_column = \"id\"\nwrite_strategy = \"append\"\n",
        fake.base_url,
        database.display(),
    )
}

#[test]
fn given_a_databricks_source_when_loading_then_its_result_files_load_every_row_once_without_the_token_and_the_cursor_reads_only_new_ones()
-> Result<(), String> {
    run_async(|| async move {
        let fake = start_fake_databricks().await?;
        fake.place_table(
            "marts.orders",
            &[
                ("id", "bigint"),
                ("label", "string"),
                ("amount", "decimal(12,2)"),
                ("at", "timestamp"),
            ],
            Vec::new(),
        );
        fake.state.lock().chunk_rows = Some(2);
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        let database = folder.path().join("out.duckdb");
        let config = folder.path().join("sqlload.toml");
        std::fs::write(&config, source_project(&fake, &database))
            .map_err(|error| error.to_string())?;
        let test_cases = [
            DatabricksSourceTestCase {
                description: "the first load reads both result files, the expired link to one fetched again",
                rows: first_orders,
                expired_links: 1,
                expected_rows: "1:a:1.25:2026-11-01 10:00:00+00,2:b:2.50:2026-11-01 11:00:00+00,3:c:3.75:2026-11-01 12:00:00+00",
                expected_read: "FROM `main`.`marts`.`orders`",
            },
            DatabricksSourceTestCase {
                description: "the next load reads only rows after the destination's maximum",
                rows: later_orders,
                expired_links: 0,
                expected_rows: "1:a:1.25:2026-11-01 10:00:00+00,2:b:2.50:2026-11-01 11:00:00+00,3:c:3.75:2026-11-01 12:00:00+00,4:d:4.00:2026-11-02 10:00:00+00,5:e:5.00:2026-11-02 11:00:00+00",
                expected_read: "FROM `main`.`marts`.`orders` WHERE `id` > 3",
            },
        ];
        for test_case in test_cases {
            {
                let mut state = fake.state.lock();
                if let Some(table) = state.tables.get_mut("marts.orders") {
                    table.rows.extend((test_case.rows)());
                }
                state.expire_downloads = test_case.expired_links;
            }
            let (succeeded, output) = run_sqlload(&config, &["load"], ("", ""))?;
            assert!(succeeded, "{}:\n{output}", test_case.description);
            assert_eq!(
                duckdb_text(&database, ORDERS)?,
                test_case.expected_rows,
                "{}",
                test_case.description
            );
            let state = fake.state.lock();
            let read = state
                .statements
                .iter()
                .rfind(|statement| statement.starts_with("SELECT `id`"))
                .cloned()
                .unwrap_or_default();
            assert!(
                read.ends_with(test_case.expected_read),
                "{}: {read}",
                test_case.description
            );
            assert_eq!(state.expire_downloads, 0, "{}", test_case.description);
        }
        let state = fake.state.lock();
        assert!(
            state.downloads.len() >= 4,
            "two files on the first load, one again, and one on the next: {:?}",
            state.downloads
        );
        assert!(
            state.downloads.iter().all(|(_, authorized)| !authorized),
            "a result link was downloaded with the Databricks token: {:?}",
            state.downloads
        );
        Ok(())
    })
}
