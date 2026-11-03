use crate::batching::models::Partition;
use crate::config::models::LoadErrorPolicy;
use crate::delivery::models::PendingLoads;
use crate::delivery::tests::helpers::{
    MINUTE_MS, SECOND_MS, SENT_MS, TIMING, offsets, partition, pending, reports,
};
use crate::delivery::tests::test_types::{
    AcknowledgedTestCase, DueTestCase, FailedLoadTestCase, ForgetTestCase, HistoryStartTestCase,
    PrefixTestCase, WarnOnceTestCase,
};
use crate::stage_api::models::LoadStatus;

#[test]
fn given_reported_loads_when_settling_then_only_the_loaded_prefix_is_confirmed() {
    let test_cases = [
        PrefixTestCase {
            description: "a later batch that loaded waits for the earlier one",
            reports: &[("2024-03-10/b.json.gz", LoadStatus::Loaded)],
            expected_confirmed: &[],
            expected_pending_files: 4,
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
            expected_pending_files: 4,
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
            expected_pending_files: 4,
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
fn given_unsettled_files_when_time_passes_then_checks_are_due_on_their_backoff() {
    let test_cases = [
        DueTestCase {
            description: "a file is not checked just under five minutes after it was sent",
            reports: &[],
            skipped: &[],
            checked_at: &[],
            failed_at: &[],
            incomplete_at: &[],
            now_seconds: 299,
            expected_overdue: &[],
        },
        DueTestCase {
            description: "a file with no report is checked five minutes after it was sent",
            reports: &[],
            skipped: &[],
            checked_at: &[],
            failed_at: &[],
            incomplete_at: &[],
            now_seconds: 300,
            expected_overdue: &["a.json.gz"],
        },
        DueTestCase {
            description: "a file reported as loading is checked like an unreported one",
            reports: &[("a.json.gz", LoadStatus::InProgress)],
            skipped: &[],
            checked_at: &[],
            failed_at: &[],
            incomplete_at: &[],
            now_seconds: 300,
            expected_overdue: &["a.json.gz"],
        },
        DueTestCase {
            description: "loaded and failed files are never checked",
            reports: &[("a.json.gz", LoadStatus::Loaded)],
            skipped: &[],
            checked_at: &[],
            failed_at: &[],
            incomplete_at: &[],
            now_seconds: 3_600,
            expected_overdue: &[],
        },
        DueTestCase {
            description: "a file the pipe received before is checked on the next poll",
            reports: &[],
            skipped: &["a.json.gz"],
            checked_at: &[],
            failed_at: &[],
            incomplete_at: &[],
            now_seconds: 0,
            expected_overdue: &["a.json.gz"],
        },
        DueTestCase {
            description: "after a check finds the file in place the next one is ten minutes later",
            reports: &[],
            skipped: &[],
            checked_at: &[300],
            failed_at: &[],
            incomplete_at: &[],
            now_seconds: 899,
            expected_overdue: &[],
        },
        DueTestCase {
            description: "a slow file keeps being checked, never failed",
            reports: &[],
            skipped: &[],
            checked_at: &[300, 900, 1_500],
            failed_at: &[],
            incomplete_at: &[],
            now_seconds: 2_100,
            expected_overdue: &["a.json.gz"],
        },
        DueTestCase {
            description: "a check that could not run is retried after a minute",
            reports: &[],
            skipped: &[],
            checked_at: &[],
            failed_at: &[300],
            incomplete_at: &[],
            now_seconds: 360,
            expected_overdue: &["a.json.gz"],
        },
        DueTestCase {
            description: "checks that keep failing back off: one, then two minutes",
            reports: &[],
            skipped: &[],
            checked_at: &[],
            failed_at: &[300, 360],
            incomplete_at: &[],
            now_seconds: 479,
            expected_overdue: &[],
        },
        DueTestCase {
            description: "an incomplete report makes a file due at once",
            reports: &[],
            skipped: &[],
            checked_at: &[],
            failed_at: &[],
            incomplete_at: &[10],
            now_seconds: 10,
            expected_overdue: &["a.json.gz"],
        },
        DueTestCase {
            description: "an incomplete report does not recheck a file checked in the last minute",
            reports: &[],
            skipped: &[],
            checked_at: &[300],
            failed_at: &[],
            incomplete_at: &[330],
            now_seconds: 330,
            expected_overdue: &[],
        },
    ];
    let keys = ["a.json.gz".to_owned()];
    for test_case in &test_cases {
        let mut loads = PendingLoads::new(LoadErrorPolicy::Halt, TIMING);
        let skipped: Vec<String> = test_case
            .skipped
            .iter()
            .map(|path| (*path).to_owned())
            .collect();
        loads.add(&keys, &skipped, offsets(&[(0, 10)]), SENT_MS);
        loads.apply(&reports(test_case.reports));
        for second in test_case.checked_at {
            loads.checked(&keys, SENT_MS + second * SECOND_MS);
        }
        for second in test_case.failed_at {
            loads.check_failed(&keys, SENT_MS + second * SECOND_MS);
        }
        for second in test_case.incomplete_at {
            loads.mark_all_due(SENT_MS + second * SECOND_MS);
        }
        assert_eq!(
            loads.overdue(SENT_MS + test_case.now_seconds * SECOND_MS),
            test_case.expected_overdue,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_received_before_files_without_history_when_checked_then_each_is_warned_about_once() {
    let test_cases = [
        WarnOnceTestCase {
            description: "a received-before file is warned about on its first check only",
            skipped: &["a.json.gz"],
            checks: 2,
            expected_warnings: &["a.json.gz"],
        },
        WarnOnceTestCase {
            description: "a file sent first by this run is never warned about",
            skipped: &[],
            checks: 2,
            expected_warnings: &[],
        },
    ];
    let keys = ["a.json.gz".to_owned()];
    for test_case in &test_cases {
        let mut loads = PendingLoads::new(LoadErrorPolicy::Halt, TIMING);
        let skipped: Vec<String> = test_case
            .skipped
            .iter()
            .map(|path| (*path).to_owned())
            .collect();
        loads.add(&keys, &skipped, offsets(&[(0, 10)]), SENT_MS);
        let warnings: Vec<String> = (0..test_case.checks)
            .flat_map(|_| loads.take_unwarned_received_before(&keys))
            .collect();
        assert_eq!(
            warnings, test_case.expected_warnings,
            "{}",
            test_case.description
        );
        assert!(loads.any_unsettled(&keys), "{}", test_case.description);
    }
}

#[test]
fn given_a_checked_file_when_deciding_to_resend_then_only_unacknowledged_files_are_sent_again() {
    let test_cases = [
        AcknowledgedTestCase {
            description: "a file only sent once is sent again",
            skipped: &[],
            resent: &[],
            expected_acknowledged: false,
        },
        AcknowledgedTestCase {
            description: "a file already sent again keeps waiting",
            skipped: &[],
            resent: &["a.json.gz"],
            expected_acknowledged: true,
        },
        AcknowledgedTestCase {
            description: "a file the pipe received before keeps waiting",
            skipped: &["/a.json.gz"],
            resent: &[],
            expected_acknowledged: true,
        },
    ];
    let keys = ["a.json.gz".to_owned()];
    for test_case in &test_cases {
        let mut loads = PendingLoads::new(LoadErrorPolicy::Halt, TIMING);
        let skipped: Vec<String> = test_case
            .skipped
            .iter()
            .map(|path| (*path).to_owned())
            .collect();
        loads.add(&keys, &skipped, offsets(&[(0, 10)]), SENT_MS);
        for key in test_case.resent {
            loads.resend(key, false);
        }
        assert_eq!(
            loads.is_acknowledged("a.json.gz"),
            test_case.expected_acknowledged,
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
            description: "a file the pipe skipped as received before is looked up 14 days back",
            skipped: &["/b.json.gz"],
            expected_lookback_minutes: 14 * 24 * 60,
        },
    ];
    for test_case in &test_cases {
        let mut loads = PendingLoads::new(LoadErrorPolicy::Halt, TIMING);
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
