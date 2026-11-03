use crate::batching::models::Partition;
use crate::config::models::LoadErrorPolicy;
use crate::delivery::models::PendingLoads;
use crate::delivery::tests::helpers::{MINUTE_MS, SENT_MS, offsets, partition, pending, reports};
use crate::delivery::tests::test_types::{
    FailedLoadTestCase, ForgetTestCase, HistoryStartTestCase, PrefixTestCase, UnreportedTestCase,
};
use crate::stage_api::models::LoadStatus;

#[test]
fn given_reported_loads_when_settling_then_only_the_loaded_prefix_is_confirmed() {
    let test_cases = [
        PrefixTestCase {
            description: "a later batch that loaded waits for the earlier one",
            reports: &[("2024-03-10/b.json.gz", LoadStatus::Loaded)],
            expected_confirmed: &[],
            expected_pending_files: 3,
        },
        PrefixTestCase {
            description: "two loaded batches in order commit the newest offset per partition",
            reports: &[
                ("2024-03-10/a1.json.gz", LoadStatus::Loaded),
                ("2024-03-10/a2.json.gz", LoadStatus::Loaded),
                ("2024-03-10/b.json.gz", LoadStatus::Loaded),
            ],
            expected_confirmed: &[(0, 20), (1, 15)],
            expected_pending_files: 1,
        },
        PrefixTestCase {
            description: "a batch is loaded only when every one of its files is",
            reports: &[
                ("2024-03-10/a1.json.gz", LoadStatus::Loaded),
                ("2024-03-10/b.json.gz", LoadStatus::Loaded),
            ],
            expected_confirmed: &[],
            expected_pending_files: 2,
        },
        PrefixTestCase {
            description: "a report path with a leading slash matches the sent key",
            reports: &[
                ("/2024-03-10/a1.json.gz", LoadStatus::Loaded),
                ("2024-03-10/a2.json.gz", LoadStatus::Loaded),
            ],
            expected_confirmed: &[(0, 10), (1, 15)],
            expected_pending_files: 2,
        },
        PrefixTestCase {
            description: "files of other writers and loads in progress settle nothing",
            reports: &[
                ("2024-03-10/other.json.gz", LoadStatus::Loaded),
                ("2024-03-10/a1.json.gz", LoadStatus::InProgress),
                ("2024-03-10/a2.json.gz", LoadStatus::Loaded),
            ],
            expected_confirmed: &[],
            expected_pending_files: 3,
        },
    ];
    for test_case in &test_cases {
        let mut loads = pending(
            &[
                (
                    &["2024-03-10/a1.json.gz", "2024-03-10/a2.json.gz"],
                    &[(0, 10), (1, 15)],
                ),
                (&["2024-03-10/b.json.gz"], &[(0, 20)]),
                (&["2024-03-10/c.json.gz"], &[(0, 30)]),
            ],
            LoadErrorPolicy::Halt,
        );
        loads.apply(&reports(test_case.reports));
        let settled = loads.settle();
        assert_eq!(
            settled.confirmed,
            offsets(test_case.expected_confirmed),
            "{}",
            test_case.description
        );
        assert_eq!(
            loads.file_count(),
            test_case.expected_pending_files,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_failed_load_when_settling_then_the_policy_halts_or_skips() {
    let test_cases = [
        FailedLoadTestCase {
            description: "halt stops at the failed file and keeps reporting it",
            policy: LoadErrorPolicy::Halt,
            reports: &[("a.json.gz", LoadStatus::Failed)],
            missing: &[],
            expected_halted: Some("a.json.gz: load failed: bad row"),
            expected_skipped: 0,
            expected_confirmed: &[],
        },
        FailedLoadTestCase {
            description: "halt treats a partial load as a failure",
            policy: LoadErrorPolicy::Halt,
            reports: &[("a.json.gz", LoadStatus::PartiallyLoaded)],
            missing: &[],
            expected_halted: Some("a.json.gz: partially loaded (0 of 3 rows): bad row"),
            expected_skipped: 0,
            expected_confirmed: &[],
        },
        FailedLoadTestCase {
            description: "skip counts the file once and commits past it",
            policy: LoadErrorPolicy::Skip,
            reports: &[("a.json.gz", LoadStatus::Failed)],
            missing: &[],
            expected_halted: None,
            expected_skipped: 1,
            expected_confirmed: &[(0, 20)],
        },
        FailedLoadTestCase {
            description: "an unreported file no longer in the stage fails under the policy",
            policy: LoadErrorPolicy::Halt,
            reports: &[],
            missing: &["a.json.gz"],
            expected_halted: Some("a.json.gz: no longer in the stage"),
            expected_skipped: 0,
            expected_confirmed: &[],
        },
    ];
    for test_case in &test_cases {
        let mut loads = pending(
            &[(&["a.json.gz"], &[(0, 10)]), (&["b.json.gz"], &[(0, 20)])],
            test_case.policy,
        );
        loads.apply(&reports(test_case.reports));
        loads.apply(&reports(&[("b.json.gz", LoadStatus::Loaded)]));
        for key in test_case.missing {
            loads.fail(key, "no longer in the stage");
        }
        let first = loads.settle();
        let second = loads.settle();
        assert_eq!(
            first.halted.as_deref(),
            test_case.expected_halted,
            "{}",
            test_case.description
        );
        assert_eq!(
            second.halted.as_deref(),
            test_case.expected_halted,
            "{}",
            test_case.description
        );
        assert_eq!(
            first.skipped.len() + second.skipped.len(),
            test_case.expected_skipped,
            "{}",
            test_case.description
        );
        assert_eq!(
            first.confirmed,
            offsets(test_case.expected_confirmed),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_unreported_files_when_time_passes_then_they_are_overdue_after_five_minutes() {
    let test_cases = [
        UnreportedTestCase {
            description: "a file is not overdue just under five minutes after it was sent",
            reports: &[],
            resent_at_minutes: &[],
            now_minute: 4,
            expected_overdue: &[],
        },
        UnreportedTestCase {
            description: "a file with no report for five minutes is overdue",
            reports: &[],
            resent_at_minutes: &[],
            now_minute: 5,
            expected_overdue: &["a.json.gz"],
        },
        UnreportedTestCase {
            description: "a file reported as loading is never overdue",
            reports: &[("a.json.gz", LoadStatus::InProgress)],
            resent_at_minutes: &[],
            now_minute: 30,
            expected_overdue: &[],
        },
        UnreportedTestCase {
            description: "sending a file again restarts its timer",
            reports: &[],
            resent_at_minutes: &[5],
            now_minute: 9,
            expected_overdue: &[],
        },
        UnreportedTestCase {
            description: "a file sent again is overdue five minutes later",
            reports: &[],
            resent_at_minutes: &[5],
            now_minute: 10,
            expected_overdue: &["a.json.gz"],
        },
    ];
    for test_case in &test_cases {
        let mut loads = pending(&[(&["a.json.gz"], &[(0, 10)])], LoadErrorPolicy::Halt);
        loads.apply(&reports(test_case.reports));
        for minute in test_case.resent_at_minutes {
            loads.resend("a.json.gz", false, SENT_MS + minute * MINUTE_MS);
        }
        let overdue = loads.overdue(SENT_MS + test_case.now_minute * MINUTE_MS);
        assert_eq!(
            overdue, test_case.expected_overdue,
            "{}",
            test_case.description
        );
        assert_eq!(
            loads.was_resent("a.json.gz"),
            !test_case.resent_at_minutes.is_empty(),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_overdue_files_when_scanning_history_then_files_received_before_look_back_a_day() {
    let test_cases = [
        HistoryStartTestCase {
            description: "a file sent by this run is looked up from just before it was sent",
            skipped: &[],
            expected_lookback_minutes: 1,
        },
        HistoryStartTestCase {
            description: "a file the pipe skipped as received before is looked up a day back",
            skipped: &["/b.json.gz"],
            expected_lookback_minutes: 24 * 60,
        },
    ];
    for test_case in &test_cases {
        let mut loads = PendingLoads::new(LoadErrorPolicy::Halt);
        let keys = ["a.json.gz".to_owned(), "b.json.gz".to_owned()];
        let skipped: Vec<String> = test_case
            .skipped
            .iter()
            .map(|path| (*path).to_owned())
            .collect();
        loads.add(&keys[..1], &[], offsets(&[(0, 10)]), SENT_MS);
        loads.add(&keys[1..], &skipped, offsets(&[(0, 20)]), SENT_MS);
        assert_eq!(
            loads.history_start(&keys),
            Some(SENT_MS - test_case.expected_lookback_minutes * MINUTE_MS),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_revoked_partitions_when_forgetting_then_their_offsets_are_not_committed() {
    let test_cases = [
        ForgetTestCase {
            description: "a batch of only revoked partitions is dropped",
            revoked: &[0],
            expected_pending_files: 1,
            expected_confirmed: &[(1, 15)],
        },
        ForgetTestCase {
            description: "a mixed batch keeps the partitions still assigned",
            revoked: &[1],
            expected_pending_files: 2,
            expected_confirmed: &[(0, 20)],
        },
    ];
    for test_case in &test_cases {
        let mut loads = pending(
            &[
                (&["a.json.gz"], &[(0, 10)]),
                (&["b.json.gz"], &[(0, 20), (1, 15)]),
            ],
            LoadErrorPolicy::Halt,
        );
        let revoked: Vec<Partition> = test_case.revoked.iter().copied().map(partition).collect();
        loads.forget(&revoked);
        assert_eq!(
            loads.file_count(),
            test_case.expected_pending_files,
            "{}",
            test_case.description
        );
        loads.apply(&reports(&[
            ("a.json.gz", LoadStatus::Loaded),
            ("b.json.gz", LoadStatus::Loaded),
        ]));
        assert_eq!(
            loads.settle().confirmed,
            offsets(test_case.expected_confirmed),
            "{}",
            test_case.description
        );
    }
}
