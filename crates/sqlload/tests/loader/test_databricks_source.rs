use sqlload_testkit::fake_databricks::main::start_fake_databricks::start_fake_databricks;

use crate::bigquery_helpers::first_orders;
use crate::databricks_helpers::{all_orders, databricks_source_project};
use crate::helpers::{duckdb_text, run_async, run_sqlload};
use crate::test_types::DatabricksSourceTestCase;

const ORDERS_COLUMNS: [(&str, &str); 4] = [
    ("id", "bigint"),
    ("label", "string"),
    ("amount", "decimal(12,2)"),
    ("at", "timestamp"),
];

const ORDERS: &str = "SELECT string_agg(CAST(id AS VARCHAR) || ':' || label || ':' || CAST(amount AS VARCHAR) || ':' || CAST(\"at\" AS VARCHAR), ',' ORDER BY id) FROM raw.marts__orders";

#[test]
fn given_a_databricks_source_when_loading_then_its_result_files_load_every_row_once_without_the_token_and_the_cursor_reads_only_new_ones()
-> Result<(), String> {
    run_async(|| async move {
        let fake = start_fake_databricks().await?;
        fake.state.lock().chunk_rows = Some(2);
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        let database = folder.path().join("out.duckdb");
        let config = folder.path().join("sqlload.toml");
        std::fs::write(&config, databricks_source_project(&fake, &database))
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
                rows: all_orders,
                expired_links: 0,
                expected_rows: "1:a:1.25:2026-11-01 10:00:00+00,2:b:2.50:2026-11-01 11:00:00+00,3:c:3.75:2026-11-01 12:00:00+00,4:d:4.00:2026-11-02 10:00:00+00,5:e:5.00:2026-11-02 11:00:00+00",
                expected_read: "FROM `main`.`marts`.`orders` WHERE `id` > 3",
            },
        ];
        for test_case in test_cases {
            fake.place_table("marts.orders", &ORDERS_COLUMNS, (test_case.rows)());
            fake.state.lock().expire_downloads = test_case.expired_links;
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
