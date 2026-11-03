use crate::naming::main::qualify_stage::qualify_stage;
use crate::naming::tests::test_types::QualifyStageTestCase;

#[test]
fn given_a_configured_stage_when_qualifying_then_bare_names_use_the_destination_schema() {
    let test_cases = [
        QualifyStageTestCase {
            description: "a bare name lives in the destination schema",
            stage: "LAKE",
            expected_name: "RAW.KAFKA.LAKE",
        },
        QualifyStageTestCase {
            description: "a qualified name is kept, so a shared stage can live elsewhere",
            stage: "LAKE_DB.PUBLIC.EXPORT_STAGE",
            expected_name: "LAKE_DB.PUBLIC.EXPORT_STAGE",
        },
    ];
    for test_case in &test_cases {
        assert_eq!(
            qualify_stage("RAW", "KAFKA", test_case.stage),
            test_case.expected_name,
            "{}",
            test_case.description
        );
    }
}
