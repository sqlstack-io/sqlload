use std::time::Duration;

use crate::loader::main::render_tally::render_tally;
use crate::loader::models::{TableLoaded, TableOutcome};
use crate::loader::tests::test_types::RenderTallyTestCase;

#[test]
fn given_a_load_when_rendering_its_tally_then_lines_align_and_the_last_counts_them() {
    let test_cases = [
        RenderTallyTestCase {
            description: "the ticket's example: names padded to one width, rows with separators, failures counted",
            applied: &[
                "created  RAW.SHOP__ORDERS  12 columns",
                "altered  RAW.SHOP__ITEMS  add column coupon text -> VARCHAR",
            ],
            outcomes: &[
                ("RAW.SHOP__ORDERS", Ok(30_000), 4_200),
                ("RAW.SHOP__ITEMS", Ok(500), 1_000),
                ("RAW.SHOP__EVENTS", Err("<reason>"), 0),
            ],
            elapsed_ms: 11_300,
            expected_output: "created  RAW.SHOP__ORDERS  12 columns\naltered  RAW.SHOP__ITEMS   add column coupon text -> VARCHAR\nloaded   RAW.SHOP__ORDERS  30,000 rows  4.2s\nloaded   RAW.SHOP__ITEMS   500 rows  1.0s\nfailed   RAW.SHOP__EVENTS  <reason>\n\n✗ Completed with failures  2 loaded, 1 failed, 30,500 rows, 11.3s",
        },
        RenderTallyTestCase {
            description: "an apply changes objects and loads nothing: no row count",
            applied: &["created  RAW.A  3 columns"],
            outcomes: &[],
            elapsed_ms: 250,
            expected_output: "created  RAW.A  3 columns\n\n✓ Completed  0.2s",
        },
        RenderTallyTestCase {
            description: "recreated is the widest verb, and a reason over several lines continues under the first",
            applied: &["recreated  RAW.A  4 columns"],
            outcomes: &[("RAW.A", Err("first line\nsecond line"), 0)],
            elapsed_ms: 1_000,
            expected_output: "recreated  RAW.A  4 columns\nfailed     RAW.A  first line\n                  second line\n\n✗ Completed with failures  1 failed, 1.0s",
        },
        RenderTallyTestCase {
            description: "a million rows",
            applied: &[],
            outcomes: &[("RAW.A", Ok(1_234_567), 2_000)],
            elapsed_ms: 2_049,
            expected_output: "loaded   RAW.A  1,234,567 rows  2.0s\n\n✓ Completed  1 loaded, 1,234,567 rows, 2.0s",
        },
    ];
    for test_case in test_cases {
        let applied: Vec<String> = test_case.applied.iter().map(ToString::to_string).collect();
        let outcomes: Vec<TableOutcome> = test_case
            .outcomes
            .iter()
            .map(|(table, loaded, elapsed_ms)| TableOutcome {
                table: table.to_string(),
                origin: None,
                loaded: loaded
                    .map(|rows| TableLoaded {
                        rows,
                        ..TableLoaded::default()
                    })
                    .map_err(ToString::to_string),
                retryable: false,
                elapsed: Duration::from_millis(*elapsed_ms),
            })
            .collect();
        let (output, _) = render_tally(
            &applied,
            &outcomes,
            Duration::from_millis(test_case.elapsed_ms),
        );
        assert_eq!(
            output.join("\n"),
            test_case.expected_output,
            "{}",
            test_case.description
        );
    }
}
