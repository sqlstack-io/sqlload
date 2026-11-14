use crate::command::_helpers::load_output::{event_lines, summary_lines, Tally};
use crate::command::tests::test_types::LoadOutputTestCase;
use crate::load_events::constants::EVENTS_SCHEMA_VERSION;
use crate::load_events::models::{EventKind, LoadEvent};

/// A table's load that committed.
fn done(rows: u64, elapsed_ms: u64, position: Option<&str>) -> EventKind {
    EventKind::TableDone {
        rows,
        elapsed_ms,
        position: position.map(str::to_owned),
        changes: Vec::new(),
    }
}

/// A table's load that failed, retryably.
fn failed(reason: &str) -> EventKind {
    EventKind::TableFailed {
        reason: reason.to_owned(),
        retryable: true,
        elapsed_ms: 1_000,
    }
}

/// A table `--fail-fast` never started.
fn not_started() -> EventKind {
    EventKind::TableNotStarted {
        reason: "not started: --fail-fast stopped the load".to_owned(),
    }
}

#[test]
fn given_load_events_when_rendering_them_then_each_table_prints_as_it_finishes_and_the_summary_closes_the_tree()
 {
    let test_cases = [
        LoadOutputTestCase {
            description: "a fail-fast run where every failure is retryable: lines in the order tables finished, then the counts with not started, and the retry and next branches",
            events: vec![
                (None, EventKind::RunStarted { selected: 13, to_load: 13 }),
                (Some("tracker.issues"), EventKind::TableStarted { strategy: "merge".to_owned() }),
                (Some("tracker.issues"), done(13, 2_200, Some("updated_at now 2026-11-14 07:42:15+00"))),
                (Some("workforce.paycodes"), done(233, 2_200, None)),
                (Some("tracker.issue_changelogs"), done(13, 2_300, Some("state now 2026-11-14T06:53:12Z"))),
                (Some("grid.stock_sheet"), failed("HTTP 503 service unavailable · 6 attempts")),
                (Some("proofs.proof_versions"), failed("HTTP 503 service unavailable · 6 attempts")),
                (Some("proofs.proofs"), done(42, 2_300, None)),
                (Some("workforce.people"), done(304, 4_600, None)),
                (Some("proofs.proof_activities"), not_started()),
                (Some("procure.invoices"), not_started()),
                (Some("files.receipts"), not_started()),
                (Some("files.prices"), not_started()),
                (Some("files.customers"), not_started()),
                (Some("files.stock_counts"), not_started()),
            ],
            name_width: 26,
            elapsed_ms: 5_400,
            exit_code: 75,
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
        },
        LoadOutputTestCase {
            description: "a run that loaded everything: one row reads as a row, thousands are grouped, and only the slowest branch follows",
            events: vec![
                (Some("raw.a"), done(1, 300, None)),
                (Some("raw.b"), done(1_309, 8_000, Some("3 files, moved to archive/receipts/"))),
            ],
            name_width: 7,
            elapsed_ms: 8_300,
            exit_code: 0,
            expected_lines: "loaded  raw.a      1 row  0.3s
loaded  raw.b  1,309 rows 8.0s  3 files, moved to archive/receipts/
✓ Completed  2 loaded, 1,310 rows, 8.3s
└── slowest      raw.b 8.0s",
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
            lines.extend(event_lines(&event, test_case.name_width));
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
