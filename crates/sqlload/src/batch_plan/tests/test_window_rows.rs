use crate::batch_plan::tests::helpers::window_report;
use crate::batch_plan::tests::test_types::WindowRowsTestCase;
use crate::project::models::OnCursorOutOfRange;
use chrono_tz::Tz;

#[test]
fn given_rows_outside_a_cursor_window_when_reading_it_then_they_are_counted_and_handled_by_the_rule()
-> Result<(), String> {
    let test_cases = [
        WindowRowsTestCase {
            description: "rows on both sides fail as an ignored filter, the first counted across batches",
            hours: &[Some(1), Some(2), Some(-30), Some(50)],
            split_at: 2,
            from: Some("2026-03-01 00:00:00"),
            to: Some("2026-03-02 00:00:00"),
            inclusive: true,
            include: false,
            rule: OnCursorOutOfRange::Fail,
            zone: Tz::UTC,
            expected_kept: 2,
            expected_nulls: 0,
            expected_text: "the function returned 2 rows outside the window it was asked for, updated_at >= 2026-03-01 00:00:00+00:00 and < 2026-03-02 00:00:00+00:00: 1 before its start and 1 after its end, 2026-02-27 18:00:00+00:00 → 2026-03-03 02:00:00+00:00, the first at row 3 (updated_at 2026-02-27 18:00:00+00:00); the API may be ignoring the filter. Loading them would move the cursor past rows not read yet. Check the request's filter: an API ignores a parameter it does not know; on_cursor_out_of_range = \"skip\" drops rows outside the window, and \"warn\" loads them.",
        },
        WindowRowsTestCase {
            description: "rows on the day the window ends are skipped as an API filtering by whole days",
            hours: &[Some(1), Some(24), Some(30)],
            split_at: 1,
            from: Some("2026-03-01 00:00:00"),
            to: Some("2026-03-02 00:00:00"),
            inclusive: true,
            include: false,
            rule: OnCursorOutOfRange::Skip,
            zone: Tz::UTC,
            expected_kept: 1,
            expected_nulls: 0,
            expected_text: "2 rows outside the window updated_at >= 2026-03-01 00:00:00+00:00 and < 2026-03-02 00:00:00+00:00: 2 after its end, 2026-03-02 00:00:00+00:00 → 2026-03-02 06:00:00+00:00; all on 2026-03-02, the day the window ends: the API may filter by whole days; skipped (on_cursor_out_of_range = \"skip\")",
        },
        WindowRowsTestCase {
            description: "rows on the start's day before it and on the end's day after it fit an API filtering by whole days",
            hours: &[Some(1), Some(10), Some(30)],
            split_at: 1,
            from: Some("2026-03-01 06:00:00"),
            to: Some("2026-03-02 06:00:00"),
            inclusive: true,
            include: false,
            rule: OnCursorOutOfRange::Fail,
            zone: Tz::UTC,
            expected_kept: 1,
            expected_nulls: 0,
            expected_text: "the function returned 2 rows outside the window it was asked for, updated_at >= 2026-03-01 06:00:00+00:00 and < 2026-03-02 06:00:00+00:00: 1 before its start and 1 after its end, 2026-03-01 01:00:00+00:00 → 2026-03-02 06:00:00+00:00, the first at row 1 (updated_at 2026-03-01 01:00:00+00:00); those before its start all on 2026-03-01, the day it starts, and those after its end all on 2026-03-02, the day it ends: the API may filter by whole days. Loading them would move the cursor past rows not read yet. If it does, set on_cursor_out_of_range = \"skip\": the windows either side read them.",
        },
        WindowRowsTestCase {
            description: "a row an hour before a start read in Paris loads under warn as an offset",
            hours: &[Some(-2), Some(3)],
            split_at: 1,
            from: Some("2026-03-01 00:00:00"),
            to: Some("2026-03-02 00:00:00"),
            inclusive: true,
            include: false,
            rule: OnCursorOutOfRange::Warn,
            zone: Tz::Europe__Paris,
            expected_kept: 2,
            expected_nulls: 0,
            expected_text: "1 row outside the window updated_at >= 2026-03-01 00:00:00+01:00 and < 2026-03-02 00:00:00+01:00: 1 before its start, at 2026-02-28 23:00:00+01:00; all within 1 hour before the window's start: an offset of 1 hour, as when the window is sent in another time zone than the API reads it in; loaded (on_cursor_out_of_range = \"warn\")",
        },
        WindowRowsTestCase {
            description: "a row at a start the window leaves out",
            hours: &[Some(0), Some(3)],
            split_at: 1,
            from: Some("2026-03-01 00:00:00"),
            to: Some("2026-03-02 00:00:00"),
            inclusive: false,
            include: false,
            rule: OnCursorOutOfRange::Fail,
            zone: Tz::UTC,
            expected_kept: 1,
            expected_nulls: 0,
            expected_text: "the function returned 1 row outside the window it was asked for, updated_at > 2026-03-01 00:00:00+00:00 and < 2026-03-02 00:00:00+00:00: 1 before its start, at 2026-03-01 00:00:00+00:00, the first at row 1 (updated_at 2026-03-01 00:00:00+00:00); all at the window's start, which it leaves out: the API may include its start. Loading them would load again rows an earlier window read. Ask for rows after ctx.cursor_value when ctx.cursor_inclusive is False; or, if the API cannot, set on_cursor_out_of_range = \"skip\": an earlier window read them.",
        },
        WindowRowsTestCase {
            description: "a row at a start the window reads is inside it",
            hours: &[Some(0), Some(3)],
            split_at: 1,
            from: Some("2026-03-01 00:00:00"),
            to: Some("2026-03-02 00:00:00"),
            inclusive: true,
            include: false,
            rule: OnCursorOutOfRange::Fail,
            zone: Tz::UTC,
            expected_kept: 2,
            expected_nulls: 0,
            expected_text: "",
        },
        WindowRowsTestCase {
            description: "a NULL cursor is not outside: the bounds skip it",
            hours: &[None, Some(3)],
            split_at: 1,
            from: Some("2026-03-01 00:00:00"),
            to: Some("2026-03-02 00:00:00"),
            inclusive: true,
            include: false,
            rule: OnCursorOutOfRange::Fail,
            zone: Tz::UTC,
            expected_kept: 1,
            expected_nulls: 1,
            expected_text: "",
        },
        WindowRowsTestCase {
            description: "a NULL cursor loads with null_cursor = \"include\"",
            hours: &[None, Some(3)],
            split_at: 1,
            from: Some("2026-03-01 00:00:00"),
            to: Some("2026-03-02 00:00:00"),
            inclusive: true,
            include: true,
            rule: OnCursorOutOfRange::Fail,
            zone: Tz::UTC,
            expected_kept: 2,
            expected_nulls: 0,
            expected_text: "",
        },
    ];
    for test_case in test_cases {
        let (kept, nulls, text) = window_report(&test_case)?;
        assert_eq!(kept, test_case.expected_kept, "{}", test_case.description);
        assert_eq!(nulls, test_case.expected_nulls, "{}", test_case.description);
        assert_eq!(text, test_case.expected_text, "{}", test_case.description);
    }
    Ok(())
}
