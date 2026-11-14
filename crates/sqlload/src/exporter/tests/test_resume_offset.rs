use rdkafka::Offset;

use crate::config::models::StartFrom;
use crate::exporter::_helpers::stream_task::resume_offset;
use crate::exporter::tests::test_types::ResumeOffsetTestCase;

#[test]
fn given_the_committed_offset_read_when_resuming_a_partition_then_a_failed_read_stays_an_error_and_never_falls_back_to_start_from()
 {
    let test_cases = [
        ResumeOffsetTestCase {
            description: "a committed offset resumes there, whatever start_from says",
            found: Ok(Some(Offset::Offset(41))),
            start_from: StartFrom::Latest,
            expected_offset: Ok(Offset::Offset(41)),
        },
        ResumeOffsetTestCase {
            description: "a group without a committed offset starts from start_from = earliest",
            found: Ok(Some(Offset::Invalid)),
            start_from: StartFrom::Earliest,
            expected_offset: Ok(Offset::Beginning),
        },
        ResumeOffsetTestCase {
            description: "a group without a committed offset starts from start_from = latest",
            found: Ok(None),
            start_from: StartFrom::Latest,
            expected_offset: Ok(Offset::End),
        },
        ResumeOffsetTestCase {
            description: "a failed read with start_from = latest is an error, not the end of the partition, which would skip records",
            found: Err("broker transport failure"),
            start_from: StartFrom::Latest,
            expected_offset: Err("broker transport failure"),
        },
        ResumeOffsetTestCase {
            description: "a failed read with start_from = earliest is an error too",
            found: Err("coordinator not available"),
            start_from: StartFrom::Earliest,
            expected_offset: Err("coordinator not available"),
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            resume_offset(test_case.found.map_err(str::to_owned), test_case.start_from),
            test_case.expected_offset.map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
}
