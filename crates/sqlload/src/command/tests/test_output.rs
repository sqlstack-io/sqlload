use crate::command::_helpers::output::{paint, wrapped};
use crate::command::tests::test_types::PaintTestCase;

#[test]
fn given_a_line_of_plan_or_load_output_when_painting_it_then_only_headings_errors_warnings_the_outcome_and_loaded_take_colour()
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
            description: "a complete plan has its label green and bold, its counts plain",
            line: "✓ Plan complete  1 selected",
            expected_line: "\x1b[32m\x1b[1m✓ Plan complete\x1b[0m  1 selected",
        },
        PaintTestCase {
            description: "a load with failures has its label red and bold",
            line: "✗ Completed with failures  1 loaded, 1 failed",
            expected_line: "\x1b[38;5;167m\x1b[1m✗ Completed with failures\x1b[0m  1 loaded, 1 failed",
        },
        PaintTestCase {
            description: "a failed table in the tally is red",
            line: "failed   RAW.A  boom",
            expected_line: "\x1b[38;5;167mfailed   RAW.A  boom\x1b[0m",
        },
        PaintTestCase {
            description: "a loaded table in the tally has only its first word green, so its position stays readable",
            line: "loaded  raw.a  13 rows 2.2s  updated_at now 2026-11-14",
            expected_line: "\x1b[32mloaded\x1b[0m  raw.a  13 rows 2.2s  updated_at now 2026-11-14",
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

#[test]
fn given_a_line_wider_than_the_terminal_when_wrapping_it_then_it_breaks_at_spaces_under_its_field()
{
    let test_cases = [
        (
            "a column note in a tree carries the branches down, its own too as more columns follow, and starts under the note",
            "│   ├── updated_at  timestamp_tz    -> TIMESTAMP  (stored as a UTC instant; the offset is not kept)",
            90,
            "│   ├── updated_at  timestamp_tz    -> TIMESTAMP  (stored as a UTC instant; the offset is\n│   │                                             not kept)",
        ),
        (
            "a failed table's reason continues under the reason, not under the verb",
            "failed  raw.returns  checks.min_rows failed: 50,000 rows staged, fewer than the 100,000 required; nothing was published",
            72,
            "failed  raw.returns  checks.min_rows failed: 50,000 rows staged, fewer\n                     than the 100,000 required; nothing was published",
        ),
        (
            "a line that fits is left alone",
            "loaded  raw.a  13 rows 2.2s",
            72,
            "loaded  raw.a  13 rows 2.2s",
        ),
    ];
    for (description, line, width, expected) in test_cases {
        let wrapped = wrapped(line, width);
        assert!(
            wrapped.iter().all(|part| part.chars().count() <= width),
            "{description}: {wrapped:#?}"
        );
        assert_eq!(wrapped.join("\n"), expected, "{description}");
    }
}
