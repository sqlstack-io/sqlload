use crate::helpers::mixed_cursor_plan;
use crate::test_types::MixedCursorTestCase;

const MIXED: &str = "--start-cursor/--end-cursor apply to every selected table, but their cursors compare different kinds of value";

#[test]
fn given_a_cursor_range_when_selected_tables_have_cursors_of_different_types_then_none_is_read_with_it()
-> Result<(), String> {
    let test_cases = [
        MixedCursorTestCase {
            description: "a date and an integer cursor: both refused, naming each table's cursor type",
            selectors: &["days", "nums"],
            start: "2026-11-02",
            expected_plan: "days (date or timestamp), nums (integer)\n│   └── fix     select tables of one cursor type per run",
            expected_absent: "day from",
        },
        MixedCursorTestCase {
            description: "a text cursor would compare a date as text, silently: refused with the rest",
            selectors: &["days", "texts"],
            start: "2026-11-02",
            expected_plan: "days (date or timestamp), texts (text)",
            expected_absent: "k from 2026-11-02",
        },
        MixedCursorTestCase {
            description: "two date cursors read the same range",
            selectors: &["days", "later"],
            start: "2026-11-02",
            expected_plan: "day from 2026-11-02 (--start-cursor)",
            expected_absent: MIXED,
        },
        MixedCursorTestCase {
            description: "a table already blocked with no known cursor type is left out, so a date cursor beside it still reads the range",
            selectors: &["days", "missing"],
            start: "2026-11-02",
            expected_plan: "day from 2026-11-02 (--start-cursor)",
            expected_absent: MIXED,
        },
        MixedCursorTestCase {
            description: "a table already blocked with a known integer cursor still counts, and keeps its own reason beside the mixed one",
            selectors: &["days", "hidden"],
            start: "2026-11-02",
            expected_plan: "cursor_column n is not written to the table",
            expected_absent: "day from",
        },
        MixedCursorTestCase {
            description: "one integer cursor takes an integer",
            selectors: &["nums"],
            start: "50",
            expected_plan: "n from 50 (--start-cursor)",
            expected_absent: MIXED,
        },
        MixedCursorTestCase {
            description: "a value that does not fit the one selected integer cursor is refused at plan",
            selectors: &["nums"],
            start: "2026-11-02",
            expected_plan: "\"2026-11-02\" is not an integer, which an integer cursor needs",
            expected_absent: "n from",
        },
        MixedCursorTestCase {
            description: "a value that does not fit the one selected date cursor is refused at plan",
            selectors: &["days"],
            start: "50",
            expected_plan: "\"50\" is not a date (YYYY-MM-DD), which a date cursor needs",
            expected_absent: "day from",
        },
    ];
    for test_case in test_cases {
        let plan = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|error| error.to_string())?
            .block_on(mixed_cursor_plan(test_case.selectors, test_case.start))?;
        assert!(
            plan.contains(test_case.expected_plan) && !plan.contains(test_case.expected_absent),
            "{}: {plan}",
            test_case.description
        );
    }
    Ok(())
}
