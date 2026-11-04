use crate::files_destination::_helpers::markers::{marker_text, parse_marker};
use crate::files_destination::models::PendingPhase;
use crate::files_destination::tests::test_types::MarkerTestCase;

#[test]
fn given_a_pending_marker_when_reading_it_then_the_phase_and_keys_come_back() {
    let replacing = marker_text(
        PendingPhase::Replacing,
        &[
            "day=1/part-a-0.parquet".to_owned(),
            "day=2/part-a-0.parquet".to_owned(),
        ],
    );
    let writing = marker_text(PendingPhase::Writing, &[]);
    let test_cases = [
        MarkerTestCase {
            description: "a load stopped while writing",
            text: writing,
            expected_marker: Ok((PendingPhase::Writing, &[])),
        },
        MarkerTestCase {
            description: "a load stopped while deleting what it replaced",
            text: replacing,
            expected_marker: Ok((
                PendingPhase::Replacing,
                &["day=1/part-a-0.parquet", "day=2/part-a-0.parquet"],
            )),
        },
        MarkerTestCase {
            description: "an unknown marker is an error, never a guess",
            text: "done\nx".to_owned(),
            expected_marker: Err("a pending marker starts with Some(\"done\")"),
        },
    ];
    for test_case in test_cases {
        let marker = parse_marker(&test_case.text);
        let read = marker
            .as_ref()
            .map(|(phase, keys)| (*phase, keys.iter().map(String::as_str).collect::<Vec<_>>()));
        let expected = test_case
            .expected_marker
            .map(|(phase, keys)| (phase, keys.to_vec()));
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
