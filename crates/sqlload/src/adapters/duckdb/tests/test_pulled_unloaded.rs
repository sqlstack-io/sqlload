use crate::adapters::duckdb::tests::helpers::pulled_unloaded;
use crate::adapters::duckdb::tests::test_types::PulledUnloadedTestCase;

#[test]
fn given_an_unload_when_duckdb_loads_its_files_itself_then_only_the_rows_it_reports_commit() {
    let test_cases = [
        PulledUnloadedTestCase {
            description: "the files hold the rows the unload reports: they commit",
            reported: 2,
            with_files: true,
            expected_outcome: "Committed(2)",
            expected_rows: 2,
        },
        PulledUnloadedTestCase {
            description: "the files hold fewer rows than the unload reports: the load fails and nothing stays",
            reported: 3,
            with_files: true,
            expected_outcome: "failed",
            expected_rows: 0,
        },
        PulledUnloadedTestCase {
            description: "an unload of no rows wrote no files: nothing to read, nothing loaded",
            reported: 0,
            with_files: false,
            expected_outcome: "Committed(0)",
            expected_rows: 0,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            pulled_unloaded(test_case.reported, test_case.with_files),
            Ok((
                test_case.expected_outcome.to_owned(),
                test_case.expected_rows
            )),
            "{}",
            test_case.description
        );
    }
}
