use crate::command::_helpers::output::paint;
use crate::command::tests::test_types::PaintTestCase;

#[test]
fn given_a_line_of_plan_or_load_output_when_painting_it_then_only_headings_errors_warnings_and_the_outcome_take_colour()
 {
    let test_cases = [
        PaintTestCase {
            description: "a section heading is bold, its command note dimmed in place",
            line: "Loads (1)      sqlload load",
            expected_line: "\x1b[1mLoads (1)\x1b[0m      \x1b[2msqlload load\x1b[0m",
        },
        PaintTestCase {
            description: "Errors are red",
            line: "Errors (2)",
            expected_line: "\x1b[38;5;167mErrors (2)\x1b[0m",
        },
        PaintTestCase {
            description: "Warnings are yellow",
            line: "Warnings (1)",
            expected_line: "\x1b[33mWarnings (1)\x1b[0m",
        },
        PaintTestCase {
            description: "a complete plan is green",
            line: "✓ Plan complete  1 selected",
            expected_line: "\x1b[32m✓ Plan complete  1 selected\x1b[0m",
        },
        PaintTestCase {
            description: "a failed table in the tally is red",
            line: "failed   RAW.A  boom",
            expected_line: "\x1b[38;5;167mfailed   RAW.A  boom\x1b[0m",
        },
        PaintTestCase {
            description: "an item, a detail and a JSON document stay plain",
            line: "{\"error\":{\"code\":\"plan\"}}",
            expected_line: "{\"error\":{\"code\":\"plan\"}}",
        },
        PaintTestCase {
            description: "a detail line stays plain",
            line: "    ├── route   read as Arrow -> DuckDB takes Arrow",
            expected_line: "    ├── route   read as Arrow -> DuckDB takes Arrow",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            paint(test_case.line.to_owned()),
            test_case.expected_line,
            "{}",
            test_case.description
        );
    }
}
