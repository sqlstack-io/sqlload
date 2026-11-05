use crate::command::_helpers::limits::blocking_threads;
use crate::command::tests::test_types::BlockingThreadsTestCase;

const PROJECT: &str = "[sources.app]\nkind = \"postgres\"\n";
const PROJECT_WITH_THREADS: &str = "threads = 10\n[sources.app]\nkind = \"postgres\"\n";

#[test]
fn given_a_thread_count_when_sizing_the_blocking_pool_then_it_is_threads_plus_64() {
    let test_cases = [
        BlockingThreadsTestCase {
            description: "the default of 4 tables",
            cli_threads: None,
            config: PROJECT,
            expected_threads: Some(68),
        },
        BlockingThreadsTestCase {
            description: "the project's threads",
            cli_threads: None,
            config: PROJECT_WITH_THREADS,
            expected_threads: Some(74),
        },
        BlockingThreadsTestCase {
            description: "--threads wins, and passes tokio's default of 512 when it must",
            cli_threads: Some(600),
            config: PROJECT_WITH_THREADS,
            expected_threads: Some(664),
        },
        BlockingThreadsTestCase {
            description: "an exporter configuration keeps the runtime's default",
            cli_threads: None,
            config: "[kafka]\nbrokers = \"b\"\n",
            expected_threads: None,
        },
        BlockingThreadsTestCase {
            description: "a file that does not parse is reported later, with the default",
            cli_threads: None,
            config: "not toml =",
            expected_threads: None,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            blocking_threads(test_case.cli_threads, test_case.config),
            test_case.expected_threads,
            "{}",
            test_case.description
        );
    }
}
