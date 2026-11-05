use crate::adapters::snowflake::main::stage::_stage_lookup::stage_lookup;
use crate::adapters::snowflake::main::stage::_stage_problem::stage_problem;
use crate::adapters::snowflake::models::ShownStage;
use crate::adapters::snowflake::tests::helpers::stage;
use crate::adapters::snowflake::tests::test_types::StageProblemTestCase;

#[test]
fn given_a_listed_stage_when_checking_it_then_only_one_on_the_url_with_the_integration_is_used() {
    let test_cases = [
        StageProblemTestCase {
            description: "the stage SQLLoad created, on the url with the integration",
            shown: &[("_SQLLOAD_STAGE", "s3://lake/raw/", "LAKE_INT")],
            expected_problem: None,
        },
        StageProblemTestCase {
            description: "a url without its trailing slash is the same folder",
            shown: &[("_SQLLOAD_STAGE", "s3://lake/raw", "lake_int")],
            expected_problem: None,
        },
        StageProblemTestCase {
            description: "a missing stage",
            shown: &[("OTHER", "s3://lake/raw/", "LAKE_INT")],
            expected_problem: Some(
                "stage _SQLLOAD_STAGE does not exist or this role cannot see it (SHOW STAGES LIKE '_SQLLOAD_STAGE' IN SCHEMA \"ANALYTICS\".\"RAW\")",
            ),
        },
        StageProblemTestCase {
            description: "a stage on another url",
            shown: &[("_SQLLOAD_STAGE", "s3://lake/other/", "LAKE_INT")],
            expected_problem: Some(
                "stage _SQLLOAD_STAGE reads s3://lake/other/, but the connection's stage url is s3://lake/raw/; point the stage at it or change the url",
            ),
        },
        StageProblemTestCase {
            description: "a stage with another integration",
            shown: &[("_SQLLOAD_STAGE", "s3://lake/raw/", "OLD_INT")],
            expected_problem: Some(
                "stage _SQLLOAD_STAGE uses storage integration OLD_INT, but the connection names lake_int; drop the stage so SQLLoad recreates it, or change storage_integration",
            ),
        },
    ];
    let stage = stage();
    let lookup = stage_lookup(&stage);
    for test_case in test_cases {
        let shown: Vec<ShownStage> = test_case
            .shown
            .iter()
            .map(|(name, url, integration)| ShownStage {
                name: (*name).to_owned(),
                url: (*url).to_owned(),
                storage_integration: (*integration).to_owned(),
            })
            .collect();
        assert_eq!(
            stage_problem(&stage, &lookup, &shown).as_deref(),
            test_case.expected_problem,
            "{}",
            test_case.description
        );
    }
}
