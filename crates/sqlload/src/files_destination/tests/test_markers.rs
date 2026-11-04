use crate::files_destination::_helpers::markers::{marker_text, parse_marker};
use crate::files_destination::models::{PendingPhase, PendingRun};
use crate::files_destination::tests::test_types::MarkerTestCase;

#[test]
fn given_a_pending_marker_when_reading_it_then_the_phase_keys_and_run_record_come_back() {
    let replacing = marker_text(
        PendingPhase::Replacing,
        &[
            "day=1/part-a-0.parquet".to_owned(),
            "day=2/part-a-0.parquet".to_owned(),
        ],
        None,
    );
    let writing = marker_text(PendingPhase::Writing, &[], None);
    let run = PendingRun {
        prefix: "_sqlload/runs/t/".to_owned(),
        name: "20260101T000000Z-a.json".to_owned(),
        json: "{\"note\":\"tab\\tand\\nnewline\"}".to_owned(),
    };
    let with_run = marker_text(
        PendingPhase::Replacing,
        &["part-old-0.parquet".to_owned()],
        Some(&run),
    );
    let test_cases = [
        MarkerTestCase {
            description: "a load stopped while writing",
            text: writing,
            expected_marker: Ok((PendingPhase::Writing, &[], None)),
        },
        MarkerTestCase {
            description: "a load stopped while deleting what it replaced",
            text: replacing,
            expected_marker: Ok((
                PendingPhase::Replacing,
                &["day=1/part-a-0.parquet", "day=2/part-a-0.parquet"],
                None,
            )),
        },
        MarkerTestCase {
            description: "a committed load keeps the run record it still has to write",
            text: with_run,
            expected_marker: Ok((
                PendingPhase::Replacing,
                &["part-old-0.parquet"],
                Some("_sqlload/runs/t/|20260101T000000Z-a.json|{\"note\":\"tab\\tand\\nnewline\"}"),
            )),
        },
        MarkerTestCase {
            description: "an unknown marker is an error, never a guess",
            text: "done\nx".to_owned(),
            expected_marker: Err("a pending marker starts with Some(\"done\")"),
        },
        MarkerTestCase {
            description: "a malformed run line is an error",
            text: "replacing\n@run\tonly-a-prefix".to_owned(),
            expected_marker: Err("a pending marker's run line is malformed"),
        },
    ];
    for test_case in test_cases {
        let marker = parse_marker(&test_case.text);
        let read = marker.as_ref().map(|marker| {
            (
                marker.phase,
                marker.keys.iter().map(String::as_str).collect::<Vec<_>>(),
                marker
                    .run
                    .as_ref()
                    .map(|run| format!("{}|{}|{}", run.prefix, run.name, run.json)),
            )
        });
        let expected = test_case
            .expected_marker
            .map(|(phase, keys, run)| (phase, keys.to_vec(), run.map(str::to_owned)));
        assert_eq!(
            read.as_ref().ok(),
            expected.as_ref().ok(),
            "{}",
            test_case.description
        );
        assert!(
            marker
                .err()
                .unwrap_or_default()
                .contains(test_case.expected_marker.err().unwrap_or_default()),
            "{}",
            test_case.description
        );
    }
}
