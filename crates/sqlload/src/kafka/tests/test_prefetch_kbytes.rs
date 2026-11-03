use crate::kafka::main::prefetch_kbytes::prefetch_kbytes;
use crate::kafka::tests::test_types::PrefetchTestCase;

const GIB: u64 = 1024 * 1024 * 1024;

#[test]
fn given_a_budget_and_consumer_count_when_splitting_then_each_share_stays_within_bounds() {
    let test_cases = [
        PrefetchTestCase {
            description: "240 consumers share 1GiB evenly",
            budget_bytes: GIB,
            consumers: 240,
            expected_kbytes: 4_369,
        },
        PrefetchTestCase {
            description: "a few consumers never exceed librdkafka's 64MiB default",
            budget_bytes: GIB,
            consumers: 2,
            expected_kbytes: 65_536,
        },
        PrefetchTestCase {
            description: "many consumers keep at least 1MiB each",
            budget_bytes: GIB,
            consumers: 5_000,
            expected_kbytes: 1_024,
        },
        PrefetchTestCase {
            description: "no streams yet counts as one consumer",
            budget_bytes: 8 * 1024 * 1024,
            consumers: 0,
            expected_kbytes: 8_192,
        },
    ];
    for test_case in &test_cases {
        assert_eq!(
            prefetch_kbytes(test_case.budget_bytes, test_case.consumers),
            test_case.expected_kbytes,
            "{}",
            test_case.description
        );
    }
}
