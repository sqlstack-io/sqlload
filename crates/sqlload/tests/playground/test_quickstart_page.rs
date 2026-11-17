use std::path::Path;

use sqlload_testkit::doc_pages::main::run_sections::run_sections;
use sqlload_testkit::doc_pages::models::PageServer;

use crate::test_types::QuickstartPageTestCase;

/// The quickstart page.
const PAGE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../website/src/content/docs/docs/quickstart.mdx"
);

/// The password the page's `docker run` gives Postgres.
#[cfg(feature = "container-tests")]
const PAGE_PASSWORD: &str = "secret";

#[test]
fn given_the_quickstart_when_running_the_playground_as_shown_then_each_output_block_matches_a_real_run()
 {
    let test_cases = [QuickstartPageTestCase {
        description: "create, check, plan, apply, load, change and load again",
        headings: &[
            "Create a playground",
            "Check, plan, apply and load",
            "Change the data and load again",
        ],
        expected_differences: &[],
    }];
    for test_case in test_cases {
        let differences = run_sections(
            Path::new(PAGE),
            Path::new(env!("CARGO_BIN_EXE_sqlload")),
            test_case.headings,
            None,
        );
        assert_eq!(
            differences,
            Ok(test_case
                .expected_differences
                .iter()
                .map(|line| (*line).to_owned())
                .collect()),
            "{}; rerun with SQLLOAD_UPDATE_DOCS=1 to rewrite the page from the run",
            test_case.description
        );
    }
}

#[cfg(feature = "container-tests")]
#[test]
fn given_the_quickstart_when_loading_its_postgres_database_as_shown_then_each_output_block_matches_a_real_run()
 {
    use sqlload_testkit::doc_pages::main::start_page_postgres::start_page_postgres;

    let test_cases = [QuickstartPageTestCase {
        description: "seed, debug, plan, apply, load, change a row and load again",
        headings: &["Point it at your own Postgres"],
        expected_differences: &[],
    }];
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    for test_case in test_cases {
        let postgres = runtime
            .block_on(start_page_postgres(PAGE_PASSWORD))
            .expect("Postgres");
        let server = PageServer {
            postgres: &postgres,
            runtime: &runtime,
        };
        let differences = run_sections(
            Path::new(PAGE),
            Path::new(env!("CARGO_BIN_EXE_sqlload")),
            test_case.headings,
            Some(server),
        );
        runtime.block_on(async { drop(postgres) });
        assert_eq!(
            differences,
            Ok(test_case
                .expected_differences
                .iter()
                .map(|line| (*line).to_owned())
                .collect()),
            "{}; rerun with SQLLOAD_UPDATE_DOCS=1 to rewrite the page from the run",
            test_case.description
        );
    }
}
