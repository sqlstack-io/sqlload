use crate::config::_helpers::kafka_support::kafka_support;
use crate::config::tests::test_types::KafkaSupportTestCase;

#[test]
fn given_a_build_with_or_without_kafka_when_parsing_an_exporter_configuration_then_only_one_without_it_fails_saying_so()
 {
    let test_cases = [
        KafkaSupportTestCase {
            description: "a build with the kafka feature parses it",
            available: true,
            expected_error: "",
        },
        KafkaSupportTestCase {
            description: "a build without it fails at parse, naming the feature and the builds that include it",
            available: false,
            expected_error: "this is an exporter configuration, which reads Kafka topics, and this build of SQLLoad has no Kafka support (it was built without the `kafka` feature); run it with the `sqlload` command or Python package, which include it",
        },
    ];
    for test_case in test_cases {
        let error = kafka_support(test_case.available)
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert_eq!(error, test_case.expected_error, "{}", test_case.description);
    }
}
