use crate::delivery::_helpers::aws_credentials::profile_credentials;
use crate::delivery::tests::test_types::ProfileCredentialsTestCase;

const FILE: &str = "[default]\naws_access_key_id = AKIADEFAULT\naws_secret_access_key = secret-a\n\n[temporary]\naws_access_key_id=ASIATEMP\naws_secret_access_key=secret-b\naws_session_token=token\n[empty]\naws_access_key_id =\n";

#[test]
fn given_a_credentials_file_when_reading_a_profile_then_only_its_keys_are_used() {
    let test_cases = [
        ProfileCredentialsTestCase {
            description: "the default profile, spaces around =",
            text: FILE,
            profile: "default",
            expected_keys: Some(("AKIADEFAULT", false)),
        },
        ProfileCredentialsTestCase {
            description: "a named profile with a session token",
            text: FILE,
            profile: "temporary",
            expected_keys: Some(("ASIATEMP", true)),
        },
        ProfileCredentialsTestCase {
            description: "a profile with empty keys has none",
            text: FILE,
            profile: "empty",
            expected_keys: None,
        },
        ProfileCredentialsTestCase {
            description: "a missing profile has none",
            text: FILE,
            profile: "prod",
            expected_keys: None,
        },
    ];
    for test_case in &test_cases {
        let found = profile_credentials(test_case.text, test_case.profile)
            .map(|credentials| (credentials.key_id, credentials.session_token.is_some()));
        assert_eq!(
            found.as_ref().map(|(key, token)| (key.as_str(), *token)),
            test_case.expected_keys,
            "{}",
            test_case.description
        );
    }
}
