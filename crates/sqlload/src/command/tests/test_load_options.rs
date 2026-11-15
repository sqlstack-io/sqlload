use crate::command::tests::helpers::load_options;
use crate::command::tests::test_types::LoadOptionsTestCase;

#[test]
fn given_selection_and_cursor_flags_when_parsing_a_load_then_the_plan_options_hold_them() {
    let test_cases = [
        LoadOptionsTestCase {
            description: "positional, --select and -s selectors combine",
            argv: &["orders", "--select", "shop", "items", "-s", "raw.events"],
            expected_options: Ok("orders,shop,items,raw.events |  |  | "),
        },
        LoadOptionsTestCase {
            description: "--exclude takes several selectors",
            argv: &["shop", "--exclude", "shop__items", "shop__events"],
            expected_options: Ok("shop | shop__items,shop__events |  | "),
        },
        LoadOptionsTestCase {
            description: "untyped cursors pass as text, typed later by each table's cursor",
            argv: &["--start-cursor", "100", "--end-cursor", "2026-11-05"],
            expected_options: Ok(" |  | 100 | 2026-11-05"),
        },
        LoadOptionsTestCase {
            description: "the -ts and -int flags combine, as in SQLBuild, each for its own cursor family",
            argv: &[
                "--start-cursor-ts",
                "2026-10-01",
                "--end-cursor-int",
                "2027",
            ],
            expected_options: Ok(" |  |  | "),
        },
        LoadOptionsTestCase {
            description: "the untyped and typed flags are refused together, in SQLBuild's words",
            argv: &["--start-cursor", "100", "--end-cursor-int", "200"],
            expected_options: Err(
                "--start-cursor/--end-cursor cannot be combined with the -ts or -int cursor flags",
            ),
        },
        LoadOptionsTestCase {
            description: "an -int value must be a whole number",
            argv: &["--start-cursor-int", "3.14"],
            expected_options: Err("--start-cursor-int value '3.14' is not a whole number"),
        },
        LoadOptionsTestCase {
            description: "a -ts value must be an RFC 3339 timestamp or a date",
            argv: &["--end-cursor-ts", "yesterday"],
            expected_options: Err(
                "--end-cursor-ts value 'yesterday' is not an RFC 3339 timestamp or a date",
            ),
        },
        LoadOptionsTestCase {
            description: "--threads is gone; --concurrency replaces it",
            argv: &["--threads", "2"],
            expected_options: Err("unexpected argument '--threads'"),
        },
    ];
    for test_case in test_cases {
        let parsed = load_options(test_case.argv);
        assert_eq!(
            parsed.clone().ok().as_deref(),
            test_case.expected_options.ok(),
            "{}: {parsed:?}",
            test_case.description
        );
        assert!(
            parsed
                .clone()
                .err()
                .unwrap_or_default()
                .contains(test_case.expected_options.err().unwrap_or_default()),
            "{}: {parsed:?}",
            test_case.description
        );
    }
}
