use crate::config::models::LoadErrorPolicy;
use crate::delivery::_helpers::copy_history::history_loads;
use crate::delivery::tests::helpers::{offsets, pending};
use crate::delivery::tests::test_types::CopyHistoryTestCase;
use crate::sql_api::models::SqlRows;

const FIRST: &str = "ORDERS/2026-11-17/orders_run1_a.json.gz";
const SECOND: &str = "ORDERS/2026-11-17/orders_run1_b.json.gz";

#[test]
fn given_copy_history_rows_when_settling_auto_ingest_files_then_only_loaded_batches_commit() {
    let test_cases = [
        CopyHistoryTestCase {
            description: "names relative to the pipe's folder, as Snowflake reports them, match the keys under the bucket prefix",
            rows: &[
                ("2026-11-17/orders_run1_a.json.gz", "Loaded", None),
                ("2026-11-17/orders_run1_b.json.gz", "Loaded", None),
            ],
            expected_confirmed: &[(0, 20)],
            expected_halted: None,
        },
        CopyHistoryTestCase {
            description: "a name with a leading slash matches too",
            rows: &[("/2026-11-17/orders_run1_a.json.gz", "Loaded", None)],
            expected_confirmed: &[(0, 10)],
            expected_halted: None,
        },
        CopyHistoryTestCase {
            description: "an earlier run's file of the same rows confirms nothing",
            rows: &[("2026-11-17/orders_run0_a.json.gz", "Loaded", None)],
            expected_confirmed: &[],
            expected_halted: None,
        },
        CopyHistoryTestCase {
            description: "a failed file halts with its first error, and the batch after it waits",
            rows: &[
                (
                    "2026-11-17/orders_run1_a.json.gz",
                    "Load failed",
                    Some("Numeric value 'x' is not recognized"),
                ),
                ("2026-11-17/orders_run1_b.json.gz", "Loaded", None),
            ],
            expected_confirmed: &[],
            expected_halted: Some(
                "ORDERS/2026-11-17/orders_run1_a.json.gz: load failed: Numeric value 'x' is not recognized",
            ),
        },
        CopyHistoryTestCase {
            description: "a skipped file loaded nothing, so it halts like a failed one",
            rows: &[("2026-11-17/orders_run1_a.json.gz", "Load skipped", None)],
            expected_confirmed: &[],
            expected_halted: Some("ORDERS/2026-11-17/orders_run1_a.json.gz: load failed"),
        },
        CopyHistoryTestCase {
            description: "a partly loaded file halts",
            rows: &[(
                "2026-11-17/orders_run1_a.json.gz",
                "Partially loaded",
                Some("bad row"),
            )],
            expected_confirmed: &[],
            expected_halted: Some(
                "ORDERS/2026-11-17/orders_run1_a.json.gz: partially loaded (2 of 3 rows): bad row",
            ),
        },
        CopyHistoryTestCase {
            description: "a load still in progress waits",
            rows: &[("2026-11-17/orders_run1_a.json.gz", "Load in progress", None)],
            expected_confirmed: &[],
            expected_halted: None,
        },
    ];
    for test_case in &test_cases {
        let mut loads = pending(
            &[(&[FIRST], &[(0, 10)]), (&[SECOND], &[(0, 20)])],
            LoadErrorPolicy::Halt,
        );
        let rows = SqlRows {
            columns: [
                "STAGE_LOCATION",
                "FILE_NAME",
                "STATUS",
                "ROW_COUNT",
                "ROW_PARSED",
                "FIRST_ERROR_MESSAGE",
            ]
            .map(str::to_owned)
            .to_vec(),
            rows: test_case
                .rows
                .iter()
                .map(|(name, status, error)| {
                    vec![
                        Some("s3://bucket/exports/ORDERS/".to_owned()),
                        Some((*name).to_owned()),
                        Some((*status).to_owned()),
                        Some("2".to_owned()),
                        Some("3".to_owned()),
                        error.map(str::to_owned),
                    ]
                })
                .collect(),
        };
        loads.apply(&history_loads(&rows, "s3://bucket/exports/"));
        let settled = loads.settle();
        assert_eq!(
            settled.confirmed,
            offsets(test_case.expected_confirmed),
            "{}",
            test_case.description
        );
        assert_eq!(
            settled.halted.as_deref(),
            test_case.expected_halted,
            "{}",
            test_case.description
        );
    }
}
