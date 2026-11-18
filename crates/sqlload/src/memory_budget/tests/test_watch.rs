use crate::memory_budget::_helpers::watch::run_stop_due;
use crate::memory_budget::tests::test_types::RunStopTestCase;

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
        let mut over = 0;
        let mut stopped = None;
        for (index, (percent, stopping)) in test_case.readings.iter().enumerate() {
            let stops;
            (over, stops) = run_stop_due(*percent, over, *stopping);
            if stops {
                stopped = Some(index);
                break;
            }
        }
        assert_eq!(
            stopped, test_case.expected_stop,
            "{}",
            test_case.description
        );
    }
}
