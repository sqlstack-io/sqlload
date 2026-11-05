use std::collections::BTreeSet;

use crate::stage_api::models::SequenceIds;
use crate::stage_api::tests::test_types::SequenceIdsTestCase;

#[test]
fn given_tables_sharing_a_session_when_they_send_requests_then_every_request_has_its_own_number() {
    let test_cases = [
        SequenceIdsTestCase {
            description: "one table's files",
            threads: 1,
            each: 5,
            expected_unique: 5,
        },
        SequenceIdsTestCase {
            description: "tables loading at once",
            threads: 4,
            each: 100,
            expected_unique: 400,
        },
    ];
    for test_case in test_cases {
        let ids = SequenceIds::default();
        let seen: BTreeSet<u64> = std::thread::scope(|scope| {
            let workers: Vec<_> = (0..test_case.threads)
                .map(|_| {
                    scope.spawn(|| {
                        (0..test_case.each)
                            .map(|_| ids.next())
                            .collect::<Vec<u64>>()
                    })
                })
                .collect();
            workers
                .into_iter()
                .flat_map(|worker| worker.join().unwrap_or_default())
                .collect()
        });
        assert_eq!(
            seen.len(),
            test_case.expected_unique,
            "{}",
            test_case.description
        );
        assert_eq!(seen.first(), Some(&1), "{}", test_case.description);
    }
}
