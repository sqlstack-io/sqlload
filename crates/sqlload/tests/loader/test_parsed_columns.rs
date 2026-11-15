use crate::files_helpers::{names_case_outcome, text_bytes};
use crate::test_types::{ColumnNamesTestCase, NamesRun};

/// Times in Paris around the change to summer time on 29 March 2026 (02:00 CET becomes 03:00 CEST): ISO and three US formats, the last with its own offset; `test_python_sources.py` loads the same values from a dict and expects the same instants.
const PARIS_TIMES: &str = "id,Updated At\n1,2026-03-29T01:30:00\n2,03/29/2026 03:30:00\n3,03/29/2026 12:00\n4,03/30/2026\n5,2026-03-29T09:00:00Z\n";

/// Each row's instant in UTC, in id order.
const UTC_INSTANTS: &str = "SELECT string_agg(CAST(timezone('UTC', updated_at) AS VARCHAR), ',' ORDER BY id) || '|' || (SELECT typeof(updated_at) FROM raw.shop_orders LIMIT 1) FROM raw.shop_orders";

#[test]
fn given_a_csv_column_of_mixed_formats_in_paris_when_loading_with_parse_then_each_value_loads_as_its_utc_instant()
-> Result<(), String> {
    let test_cases = [
        ColumnNamesTestCase {
            description: "the source's timezone reads the times without an offset, by the column's source spelling, and plan says where the zone comes from",
            settings: "format = \"csv\"\nwrite_strategy = \"append\"\ntimezone = \"Europe/Paris\"\n[sources.ShopOrders.tables.ShopOrders]\nparse = { \"Updated At\" = { timestamp = [\"iso\", \"%m/%d/%Y %H:%M:%S\", \"%m/%d/%Y %H:%M\", \"%m/%d/%Y\"] } }\n",
            runs: &[NamesRun {
                file: "a.csv",
                contents: PARIS_TIMES,
                encode: text_bytes,
                expected_ok: true,
                expected_plan: "├── parse   updated_at  timestamp_tz from iso, \"%m/%d/%Y %H:%M:%S\", \"%m/%d/%Y %H:%M\", \"%m/%d/%Y\"; times without an offset in Europe/Paris (the source's timezone)",
            }],
            expected_columns: ("shop_orders", "id,updated_at"),
            check: (
                UTC_INSTANTS,
                "2026-03-29 00:30:00,2026-03-29 01:30:00,2026-03-29 10:00:00,2026-03-29 22:00:00,2026-03-29 09:00:00|TIMESTAMP WITH TIME ZONE",
            ),
        },
        ColumnNamesTestCase {
            description: "the column's tz wins over the source's timezone, and a value with an offset keeps its instant",
            settings: "format = \"csv\"\nwrite_strategy = \"append\"\ntimezone = \"America/New_York\"\n[sources.ShopOrders.tables.ShopOrders]\nparse = { updated_at = { timestamp = [\"iso\", \"%m/%d/%Y %H:%M:%S\", \"%m/%d/%Y %H:%M\", \"%m/%d/%Y\"], tz = \"Europe/Paris\" } }\n",
            runs: &[NamesRun {
                file: "a.csv",
                contents: PARIS_TIMES,
                encode: text_bytes,
                expected_ok: true,
                expected_plan: "times without an offset in Europe/Paris (its tz)",
            }],
            expected_columns: ("shop_orders", "id,updated_at"),
            check: (
                UTC_INSTANTS,
                "2026-03-29 00:30:00,2026-03-29 01:30:00,2026-03-29 10:00:00,2026-03-29 22:00:00,2026-03-29 09:00:00|TIMESTAMP WITH TIME ZONE",
            ),
        },
        ColumnNamesTestCase {
            description: "without tz or timezone, times without an offset are UTC",
            settings: "format = \"csv\"\nwrite_strategy = \"append\"\n[sources.ShopOrders.tables.ShopOrders]\nparse = { updated_at = { timestamp = [\"iso\", \"%m/%d/%Y %H:%M:%S\", \"%m/%d/%Y %H:%M\", \"%m/%d/%Y\"] } }\n",
            runs: &[NamesRun {
                file: "a.csv",
                contents: PARIS_TIMES,
                encode: text_bytes,
                expected_ok: true,
                expected_plan: "times without an offset in UTC (the default, as neither tz nor timezone is set)",
            }],
            expected_columns: ("shop_orders", "id,updated_at"),
            check: (
                UTC_INSTANTS,
                "2026-03-29 01:30:00,2026-03-29 03:30:00,2026-03-29 12:00:00,2026-03-30 00:00:00,2026-03-29 09:00:00|TIMESTAMP WITH TIME ZONE",
            ),
        },
    ];
    for test_case in &test_cases {
        let outcome = names_case_outcome(test_case)?;
        let expected_runs: Vec<(bool, bool, bool)> = test_case
            .runs
            .iter()
            .map(|run| (run.expected_ok, run.expected_ok, true))
            .collect();
        assert_eq!(
            (
                outcome.runs,
                outcome.columns.as_str(),
                outcome.checked.as_str()
            ),
            (
                expected_runs,
                test_case.expected_columns.1,
                test_case.check.1
            ),
            "{}: {}",
            test_case.description,
            outcome.output
        );
    }
    Ok(())
}
