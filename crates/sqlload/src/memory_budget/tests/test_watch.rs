use crate::memory_budget::_helpers::watch::run_stop_due;
use crate::memory_budget::main::start_watchdog::start_watchdog;
use crate::memory_budget::main::watch_child::watch_child;
use crate::memory_budget::models::{BudgetOrigin, MemoryBudget};
use crate::memory_budget::tests::test_types::{RunStopTestCase, WatchedChildTestCase};

#[test]
fn given_readings_past_95_percent_when_watching_then_the_run_stops_only_when_no_table_stopping_frees_memory_in_time()
 {
    let test_cases = [
        RunStopTestCase {
            description: "95% with no table to stop: the run stops at once",
            readings: vec![(80, false), (93, false), (95, false)],
            expected_stop: Some(2),
        },
        RunStopTestCase {
            description: "94% with no table to stop never stops the run",
            readings: vec![(94, false); 200],
            expected_stop: None,
        },
        RunStopTestCase {
            description: "a table stopping at 96% gets 50 readings: the 51st past them stops the run",
            readings: vec![(96, true); 60],
            expected_stop: Some(50),
        },
        RunStopTestCase {
            description: "memory falling to 92% after 95% keeps counting, and 95% again past the grace stops the run",
            readings: [vec![(96, true)], vec![(92, true); 55], vec![(95, true)]].concat(),
            expected_stop: Some(56),
        },
        RunStopTestCase {
            description: "memory falling under 90% resets the count: 95% again starts a new grace",
            readings: [
                vec![(96, true); 40],
                vec![(85, false)],
                vec![(96, true); 40],
            ]
            .concat(),
            expected_stop: None,
        },
        RunStopTestCase {
            description: "92% for long, never 95%: no count, so a later 95% while a table stops waits its grace",
            readings: [vec![(92, true); 100], vec![(95, true); 3]].concat(),
            expected_stop: None,
        },
        RunStopTestCase {
            description: "the stopping table ends without memory coming down: the next 95% reading stops the run",
            readings: [vec![(96, true); 10], vec![(96, false)]].concat(),
            expected_stop: Some(10),
        },
    ];
    for test_case in test_cases {
        let stopped = test_case
            .readings
            .iter()
            .scan(0, |over, (percent, stopping)| {
                let stops;
                (*over, stops) = run_stop_due(*percent, *over, *stopping);
                Some(stops)
            })
            .position(|stops| stops);
        assert_eq!(
            stopped, test_case.expected_stop,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_watched_children_when_some_handles_drop_then_the_guard_counts_only_the_others() {
    let test_cases = [
        WatchedChildTestCase {
            description: "both children count while open",
            pids: &[41, 42],
            dropped: 0,
            expected_counted: &[41, 42],
        },
        WatchedChildTestCase {
            description: "an ended child's id stops counting",
            pids: &[41, 42],
            dropped: 1,
            expected_counted: &[42],
        },
        WatchedChildTestCase {
            description: "no child counts once all have ended",
            pids: &[41, 42],
            dropped: 2,
            expected_counted: &[],
        },
    ];
    for test_case in test_cases {
        let guard = start_watchdog(MemoryBudget {
            bytes: u64::MAX,
            origin: BudgetOrigin::Setting,
            limit: None,
            overhead: 0,
            duckdb: None,
            duckdb_targets: 0,
            table_share: 0,
            tables_at_once: 1,
        });
        let mut handles: Vec<_> = test_case
            .pids
            .iter()
            .map(|pid| watch_child(&guard, *pid))
            .collect();
        handles.drain(..test_case.dropped);
        let counted = guard
            .children
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert_eq!(
            counted, test_case.expected_counted,
            "{}",
            test_case.description
        );
    }
}
