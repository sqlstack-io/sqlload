use crate::adapters::snowflake::tests::helpers::unload_blocked_for;
use crate::adapters::snowflake::tests::test_types::UnloadBlockedTestCase;

const OUTSIDE: &str =
    "Snowflake reads another connection's unload itself only through the target's external stage";

#[test]
fn given_a_snowflake_sources_unload_when_routing_into_snowflake_then_snowflake_copies_only_what_the_targets_external_stage_covers()
 {
    let test_cases = [
        UnloadBlockedTestCase {
            description: "an S3 folder below the target's external stage is copied by Snowflake",
            unload_to: Some("s3://lake/raw/unload"),
            external: true,
            masks: false,
            expected_blocked: None,
        },
        UnloadBlockedTestCase {
            description: "the stage's own folder, written with a slash, is covered too",
            unload_to: Some("s3://lake/raw/"),
            external: true,
            masks: false,
            expected_blocked: None,
        },
        UnloadBlockedTestCase {
            description: "another folder of the same bucket goes through SQLLoad",
            unload_to: Some("s3://lake/elsewhere/"),
            external: true,
            masks: false,
            expected_blocked: Some(OUTSIDE),
        },
        UnloadBlockedTestCase {
            description: "a folder whose name only starts like the stage's goes through SQLLoad",
            unload_to: Some("s3://lake/rawish/"),
            external: true,
            masks: false,
            expected_blocked: Some(OUTSIDE),
        },
        UnloadBlockedTestCase {
            description: "the same folder in another bucket goes through SQLLoad",
            unload_to: Some("s3://other/raw/unload/"),
            external: true,
            masks: false,
            expected_blocked: Some(OUTSIDE),
        },
        UnloadBlockedTestCase {
            description: "an S3 unload into a target without an external stage goes through SQLLoad",
            unload_to: Some("s3://lake/raw/unload/"),
            external: false,
            masks: false,
            expected_blocked: Some(OUTSIDE),
        },
        UnloadBlockedTestCase {
            description: "the source's user stage is not the target's to read",
            unload_to: None,
            external: true,
            masks: false,
            expected_blocked: Some(OUTSIDE),
        },
        UnloadBlockedTestCase {
            description: "a named stage of the source's is not the target's to read",
            unload_to: Some("@ops.loading.unload"),
            external: true,
            masks: false,
            expected_blocked: Some(OUTSIDE),
        },
        UnloadBlockedTestCase {
            description: "masks need SQLLoad to see the rows, even under the stage",
            unload_to: Some("s3://lake/raw/unload/"),
            external: true,
            masks: true,
            expected_blocked: Some("the table masks columns"),
        },
    ];
    for test_case in &test_cases {
        let blocked = unload_blocked_for(test_case);
        match test_case.expected_blocked {
            None => assert_eq!(blocked, None, "{}", test_case.description),
            Some(expected) => assert!(
                blocked
                    .as_deref()
                    .is_some_and(|reason| reason.starts_with(expected)),
                "{}: {blocked:?}",
                test_case.description
            ),
        }
    }
}
