use sqlload_testkit::fake_bigquery::main::start_fake_bigquery::start_fake_bigquery;
use sqlload_testkit::fake_bigquery::models::FakeReadSession;

use crate::bigquery_helpers::{first_orders, later_orders, source_project};
use crate::helpers::{duckdb_text, run_async, run_sqlload};
use crate::test_types::BigQuerySourceTestCase;

const ORDERS: &str = "SELECT string_agg(CAST(id AS VARCHAR) || ':' || label || ':' || CAST(amount AS VARCHAR) || ':' || CAST(\"at\" AS VARCHAR), ',' ORDER BY id) FROM raw.marts__orders";

#[test]
fn given_a_bigquery_source_when_loading_then_its_read_streams_load_every_row_once_and_the_cursor_reads_only_new_ones()
-> Result<(), String> {
    run_async(async {
        let fake = start_fake_bigquery().await?;
        fake.place_table(
            "marts.orders",
            &[
                ("id", "INT64 NOT NULL"),
                ("label", "STRING(20)"),
                ("amount", "NUMERIC(12,2)"),
                ("at", "TIMESTAMP"),
            ],
        );
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        let database = folder.path().join("out.duckdb");
        let config = folder.path().join("sqlload.toml");
        std::fs::write(&config, source_project(&fake, &database))
            .map_err(|error| error.to_string())?;
        let test_cases = [
            BigQuerySourceTestCase {
                description: "the first load reads every row through two streams, and a stream that breaks resumes after the rows it already sent",
                rows: first_orders,
                broken_reads: 1,
                expected_rows: "1:a:1.25:2026-11-01 10:00:00+00,2:b:2.50:2026-11-01 11:00:00+00,3:c:3.75:2026-11-01 12:00:00+00",
                expected_restriction: "",
            },
            BigQuerySourceTestCase {
                description: "the next load reads only rows after the destination's maximum, the integer bound written bare as BigQuery needs",
                rows: later_orders,
                broken_reads: 0,
                expected_rows: "1:a:1.25:2026-11-01 10:00:00+00,2:b:2.50:2026-11-01 11:00:00+00,3:c:3.75:2026-11-01 12:00:00+00,4:d:4.00:2026-11-02 10:00:00+00,5:e:5.00:2026-11-02 11:00:00+00",
                expected_restriction: "`id` > 3",
            },
        ];
        for test_case in test_cases {
            fake.place_rows("marts.orders", &(test_case.rows)());
            fake.state.lock().broken_reads = test_case.broken_reads;
            let (succeeded, output) = run_sqlload(&config, &["load"], ("", ""))?;
            assert!(succeeded, "{}:\n{output}", test_case.description);
            assert_eq!(
                duckdb_text(&database, ORDERS)?,
                test_case.expected_rows,
                "{}",
                test_case.description
            );
            let session = fake.state.lock().read_sessions.last().cloned();
            assert_eq!(
                session,
                Some(FakeReadSession {
                    table: "marts.orders".to_owned(),
                    fields: ["id", "label", "amount", "at"].map(str::to_owned).to_vec(),
                    restriction: test_case.expected_restriction.to_owned(),
                    snapshot: true,
                }),
                "{}",
                test_case.description
            );
        }
        let state = fake.state.lock();
        assert!(
            state.read_calls.iter().any(|(_, offset)| *offset == 1),
            "the broken stream was read again from row 1: {:?}",
            state.read_calls
        );
        assert_eq!(
            state.unrouted_calls, 0,
            "every Storage API call carried its routing header"
        );
        Ok(())
    })
}
