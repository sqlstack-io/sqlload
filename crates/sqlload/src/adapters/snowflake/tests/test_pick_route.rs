use crate::adapters::snowflake::tests::helpers::picked;
use crate::adapters::snowflake::tests::test_types::PickRouteTestCase;

#[test]
fn given_what_each_step_found_when_choosing_then_the_first_usable_wins_and_a_forced_one_never_falls_back()
 {
    let test_cases = [
        PickRouteTestCase {
            description: "the first step works",
            forced: false,
            outcomes: &[("@~", None)],
            expected_choice: Ok(0),
        },
        PickRouteTestCase {
            description: "steps that cannot be used are passed over, with their reasons",
            forced: false,
            outcomes: &[
                ("@~", Some((true, "denied"))),
                ("@%table", Some((true, "denied"))),
                ("Snowpipe Streaming", None),
            ],
            expected_choice: Ok(2),
        },
        PickRouteTestCase {
            description: "a forced step fails at plan with its reason",
            forced: true,
            outcomes: &[("@~", Some((true, "denied")))],
            expected_choice: Err("stage @~ cannot be used: denied"),
        },
        PickRouteTestCase {
            description: "a failed check fails the plan: a blip says nothing about the step",
            forced: false,
            outcomes: &[("@~", Some((false, "timed out"))), ("@%table", None)],
            expected_choice: Err("cannot check whether @~ can be used: timed out"),
        },
        PickRouteTestCase {
            description: "a forced step's failed check fails the same way as an unusable one",
            forced: true,
            outcomes: &[("@~", Some((false, "timed out")))],
            expected_choice: Err("stage @~ cannot be used: timed out"),
        },
        PickRouteTestCase {
            description: "nothing usable names every reason",
            forced: false,
            outcomes: &[("@~", Some((true, "denied")))],
            expected_choice: Err("no route can load this table: @~ cannot be used: denied"),
        },
    ];
    for test_case in test_cases {
        let choice = picked(test_case.forced, test_case.outcomes);
        assert_eq!(
            choice.map(|(_, passed)| passed.len()),
            test_case.expected_choice.map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
}
