use crate::loader::_helpers::table_plan::blocked_table;
use crate::loader::main::render_plan::render_plan;
use crate::loader::models::{ProjectPlan, PythonTable, SkippedTable};
use crate::loader::tests::helpers::{
    merge_failing_repeats, merge_newest, python_spec, replace_mode, streamed_route, unload_spec,
    unload_store,
};
use crate::loader::tests::test_types::{
    ModedTable, MostWindowsTestCase, PlanLinesTestCase, UnloadStoreTestCase,
};

#[test]
fn given_tables_when_printing_the_plan_without_verbose_then_only_their_own_lines_show_grouped_by_source()
 {
    let test_cases = [
        PlanLinesTestCase {
            description: "a table that keeps every default is one line under its source, with no source, route or memory line",
            tables: &[ModedTable {
                name: "orders",
                mode: replace_mode,
            }],
            skipped: &[],
            expected_output: "Loads (1)  into DuckDB dev                                                      sqlload load\n\u{2514}\u{2500}\u{2500} s sources/s.py  into RAW\n    \u{2514}\u{2500}\u{2500} orders   replace\n\n\u{2713} Plan complete  1 to load",
        },
        PlanLinesTestCase {
            description: "names aligned across the section and sorted, keys after the strategy, and only what differs from the defaults as child lines",
            tables: &[
                ModedTable {
                    name: "people",
                    mode: merge_newest,
                },
                ModedTable {
                    name: "issue_changelogs",
                    mode: merge_failing_repeats,
                },
            ],
            skipped: &[],
            expected_output: "Loads (2)  into DuckDB dev                                                      sqlload load\n\u{2514}\u{2500}\u{2500} s sources/s.py  into RAW\n    \u{251c}\u{2500}\u{2500} issue_changelogs   merge on tenant_id, id\n    \u{2502}   \u{2514}\u{2500}\u{2500} repeated keys fail\n    \u{2514}\u{2500}\u{2500} people             merge on id\n        \u{2514}\u{2500}\u{2500} newest by updated_at\n\n\u{2713} Plan complete  2 to load",
        },
        PlanLinesTestCase {
            description: "skipped tables: one line per reason, named without their schema, file counts kept",
            tables: &[],
            skipped: &[
                ("files.receipts", "no new files in drop/receipts/"),
                (
                    "files.estimates",
                    "up to date: 0 new of 3 files in data/drop/estimates/ (csv)",
                ),
                ("files.prices", "no new files in drop/prices/"),
                (
                    "files.kpis",
                    "up to date: 0 new of 1 files in data/drop/kpis/ (xlsx)",
                ),
            ],
            expected_output: "Skipped (4)\n\u{251c}\u{2500}\u{2500} no new files   prices, receipts\n\u{2514}\u{2500}\u{2500} up to date     estimates (3 files), kpis (1 file)\n\n\u{2713} Plan complete  4 skipped",
        },
    ];
    let mut spec = python_spec("{}").expect("spec");
    spec.python = Some(PythonTable {
        module: "sources/s.py".to_owned(),
        declared: Vec::new(),
        lists: Default::default(),
        literal_paths: None,
    });
    for test_case in test_cases {
        let tables = test_case
            .tables
            .iter()
            .map(|moded| {
                let mut spec = spec.clone();
                spec.destination.table = format!("s__{}", moded.name);
                let mut table = blocked_table(&spec, Vec::new());
                table.schema.blocked = None;
                table.route = Some(streamed_route());
                table.mode = (moded.mode)();
                table
            })
            .collect();
        let skipped = test_case
            .skipped
            .iter()
            .map(|(destination, reason)| SkippedTable {
                source_name: "files".to_owned(),
                source_table: (*destination).to_owned(),
                destination: (*destination).to_owned(),
                reason: (*reason).to_owned(),
            })
            .collect();
        let plan = ProjectPlan {
            tables,
            skipped,
            ..ProjectPlan::default()
        };
        assert_eq!(
            render_plan(&plan, false).0.join("\n"),
            test_case.expected_output,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_snowflake_stage_url_when_resolving_its_table_then_the_unload_store_is_a_folder_ending_in_a_slash()
 {
    let test_cases = [
        UnloadStoreTestCase {
            description: "a url without a trailing slash is the folder, so an external location of that folder covers it",
            url: "s3://lake/unload",
            expected_store: "Ok(\"ObjectStore(\\\"s3://lake/unload/\\\")\")",
        },
        UnloadStoreTestCase {
            description: "a url with a trailing slash keeps it",
            url: "s3://lake/unload/",
            expected_store: "Ok(\"ObjectStore(\\\"s3://lake/unload/\\\")\")",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            format!("{:?}", unload_store(test_case.url)),
            test_case.expected_store,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_table_when_splitting_its_read_into_windows_then_an_unloaded_table_takes_fewer_so_its_merge_row_numbers_fit_64_bits()
 {
    let test_cases = [
        MostWindowsTestCase {
            description: "a table SQLLoad reads takes up to 100,000 windows",
            spec: || python_spec("{}"),
            expected_most: Ok(100_000),
        },
        MostWindowsTestCase {
            description: "an unloaded table's rows number from the window times 2^48, so it takes up to 32,767",
            spec: || unload_spec("s3://lake/unload/"),
            expected_most: Ok(32_767),
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            (test_case.spec)()
                .map(|spec| spec.most_windows())
                .map_err(|_| "no spec"),
            test_case.expected_most,
            "{}",
            test_case.description
        );
    }
}
