use crate::command::_helpers::load_output::{Tally, event_lines, summary_lines};
use crate::command::tests::helpers::{done, read_back};
use crate::command::tests::test_types::{LoadEventsFileTestCase, LoadOutputTestCase};
use crate::load_events::constants::EVENTS_SCHEMA_VERSION;
use crate::load_events::models::{EventKind, LoadEvent};

/// A `--fail-fast` run where every failure is retryable, as `--events jsonl` writes it.
const FAIL_FAST_EVENTS: &str = include_str!("fixtures/fail_fast_retryable.jsonl");

#[test]
fn given_load_events_when_rendering_them_then_each_table_prints_as_it_finishes_and_the_summary_closes_the_tree()
 {
    let test_cases = [
        LoadOutputTestCase {
            description: "a run that loaded everything: one row reads as a row, thousands are grouped, and only the slowest branch follows",
            events: vec![
                (Some("raw.a"), done(1, 300, None)),
                (
                    Some("raw.b"),
                    done(1_309, 8_000, Some("3 files, moved to archive/receipts/")),
                ),
            ],
            name_width: 7,
            rows_width: 5,
            elapsed_ms: 8_300,
            exit_code: 0,
            expected_lines: "loaded  raw.a      1 row  0.3s
loaded  raw.b  1,309 rows 8.0s  3 files, moved to archive/receipts/
✓ Completed  2 loaded, 1,310 rows, 8.3s
└── slowest      raw.b 8.0s",
        },
        LoadOutputTestCase {
            description: "a child table made as its parent loads, named wider than the column, keeps two spaces before its detail",
            events: vec![(
                Some("raw.a"),
                EventKind::TableDone {
                    rows: 1,
                    elapsed_ms: 300,
                    position: None,
                    changes: vec!["created  raw.a__items  6 columns".to_owned()],
                },
            )],
            name_width: 7,
            rows_width: 5,
            elapsed_ms: 400,
            exit_code: 0,
            expected_lines: "created  raw.a__items  6 columns
loaded  raw.a      1 row  0.3s
✓ Completed  1 loaded, 1 row, 0.4s
└── slowest      raw.a 0.3s",
        },
        LoadOutputTestCase {
            description: "counts of 100,000 and more line up with the rest when the plan expects them",
            events: vec![
                (Some("raw.a"), done(123_456, 2_000, None)),
                (Some("raw.b"), done(1, 100, None)),
            ],
            name_width: 7,
            rows_width: 7,
            elapsed_ms: 2_200,
            exit_code: 0,
            expected_lines: "loaded  raw.a  123,456 rows 2.0s
loaded  raw.b        1 row  0.1s
✓ Completed  2 loaded, 123,457 rows, 2.2s
└── slowest      raw.a 2.0s",
        },
    ];
    for test_case in test_cases {
        let mut tally = Tally::default();
        let mut lines: Vec<String> = Vec::new();
        for (table, kind) in test_case.events {
            let event = LoadEvent {
                schema_version: EVENTS_SCHEMA_VERSION,
                run_id: "run".to_owned(),
                at: "2026-11-14T00:00:00.000Z".to_owned(),
                table: table.map(str::to_owned),
                kind,
            };
            tally.add(&event);
            lines.extend(event_lines(
                &event,
                (test_case.name_width, test_case.rows_width),
            ));
        }
        lines.extend(summary_lines(
            &tally,
            test_case.elapsed_ms,
            test_case.exit_code,
        ));
        assert_eq!(
            lines.join("\n"),
            test_case.expected_lines,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_the_events_of_a_fail_fast_run_as_jsonl_when_read_back_then_each_line_is_unchanged_and_renders_the_exit_75_summary()
-> Result<(), String> {
    let test_cases = [LoadEventsFileTestCase {
        description: "a --fail-fast run where every failure is retryable, as --events jsonl writes it",
        events: FAIL_FAST_EVENTS,
        name_width: 26,
        rows_width: 5,
        expected_lines: "loaded  tracker.issues               13 rows 2.2s  updated_at now 2026-11-14 07:42:15+00
loaded  workforce.paycodes          233 rows 2.2s
loaded  tracker.issue_changelogs     13 rows 2.3s  state now 2026-11-14T06:53:12Z
failed  grid.stock_sheet          HTTP 503 service unavailable · 6 attempts
failed  proofs.proof_versions     HTTP 503 service unavailable · 6 attempts
loaded  proofs.proofs                42 rows 2.3s
loaded  workforce.people            304 rows 4.6s
✗ Completed with failures  5 loaded, 2 failed, 6 not started, 605 rows, 5.4s
├── not started  proofs.proof_activities, procure.invoices, files.receipts, +3 more (--fail-fast)
├── slowest      workforce.people 4.6s
├── retry        every failure is retryable, so exit 75: run the same command again
└── next         the 8 unchanged tables load; the 5 that committed carry on from their positions",
    }];
    for test_case in test_cases {
        let (written, lines) = read_back(
            test_case.events,
            (test_case.name_width, test_case.rows_width),
        )?;
        assert_eq!(
            written,
            test_case.events.lines().collect::<Vec<&str>>(),
            "{}: each event writes back as it was read",
            test_case.description
        );
        assert_eq!(lines, test_case.expected_lines, "{}", test_case.description);
    }
    Ok(())
}
