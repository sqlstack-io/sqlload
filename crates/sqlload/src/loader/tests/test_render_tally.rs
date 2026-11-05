use std::time::Duration;

use crate::loader::main::render_tally::render_tally;
use crate::loader::models::TallyLine;
use crate::loader::tests::test_types::RenderTallyTestCase;

#[test]
fn given_a_load_when_rendering_its_tally_then_lines_align_and_the_last_counts_them() {
    let test_cases = [
        RenderTallyTestCase {
            description: "the ticket's example: names padded to one width, rows with separators, failures counted",
            lines: &[
                ("created", "RAW.SHOP__ORDERS", "12 columns"),
                (
                    "altered",
                    "RAW.SHOP__ITEMS",
                    "add column coupon text -> VARCHAR",
                ),
                ("loaded", "RAW.SHOP__ORDERS", "30,000 rows  4.2s"),
                ("loaded", "RAW.SHOP__ITEMS", "500 rows  1.0s"),
                ("failed", "RAW.SHOP__EVENTS", "<reason>"),
            ],
            rows: 30_500,
            elapsed_ms: 11_300,
            expected_output: "created  RAW.SHOP__ORDERS  12 columns\naltered  RAW.SHOP__ITEMS   add column coupon text -> VARCHAR\nloaded   RAW.SHOP__ORDERS  30,000 rows  4.2s\nloaded   RAW.SHOP__ITEMS   500 rows  1.0s\nfailed   RAW.SHOP__EVENTS  <reason>\n\n✗ Completed with failures  2 loaded, 1 failed, 30,500 rows, 11.3s",
        },
        RenderTallyTestCase {
            description: "an apply changes objects and loads nothing: no row count",
            lines: &[("created", "RAW.A", "3 columns")],
            rows: 0,
            elapsed_ms: 250,
            expected_output: "created  RAW.A  3 columns\n\n✓ Completed  0.2s",
        },
        RenderTallyTestCase {
            description: "a million rows",
            lines: &[("loaded", "RAW.A", "1,234,567 rows  2.0s")],
            rows: 1_234_567,
            elapsed_ms: 2_049,
            expected_output: "loaded   RAW.A  1,234,567 rows  2.0s\n\n✓ Completed  1 loaded, 1,234,567 rows, 2.0s",
        },
    ];
    for test_case in test_cases {
        let lines: Vec<TallyLine> = test_case
            .lines
            .iter()
            .map(|(verb, name, detail)| TallyLine {
                verb: *verb,
                name: (*name).to_owned(),
                detail: (*detail).to_owned(),
            })
            .collect();
        let (output, _) = render_tally(
            &lines,
            (test_case.rows, Duration::from_millis(test_case.elapsed_ms)),
        );
        assert_eq!(
            output.join("\n"),
            test_case.expected_output,
            "{}",
            test_case.description
        );
    }
}
