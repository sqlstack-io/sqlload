use object_store::aws::AmazonS3ConfigKey;

use crate::auth::models::Secret;
use crate::file_store::_helpers::aws_keys::{key_source, keyed_builder, profile_credentials};
use crate::file_store::tests::test_types::{
    KeySourceTestCase, KeyedBuilderTestCase, ProfileCredentialsTestCase,
};
use crate::stage_api::models::AwsCredentials;

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

#[test]
fn given_keys_in_the_environment_or_a_shared_file_when_choosing_an_s3_clients_keys_then_the_first_in_the_chain_wins()
-> Result<(), String> {
    let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
    std::fs::write(directory.path().join("credentials"), FILE)
        .map_err(|error| error.to_string())?;
    let test_cases = [
        KeySourceTestCase {
            description: "keys in the environment win over the file",
            profile: Some("temporary"),
            credentials_file: "credentials",
            variables: &[
                ("AWS_ACCESS_KEY_ID", "AKIAENV"),
                ("AWS_SECRET_ACCESS_KEY", "s"),
            ],
            expected_text: "AWS_ACCESS_KEY_ID in the environment",
        },
        KeySourceTestCase {
            description: "without them, the file's default profile",
            profile: None,
            credentials_file: "credentials",
            variables: &[],
            expected_text: "credentials, profile default",
        },
        KeySourceTestCase {
            description: "the connection's profile wins over AWS_PROFILE",
            profile: Some("temporary"),
            credentials_file: "credentials",
            variables: &[("AWS_PROFILE", "default")],
            expected_text: "credentials, profile temporary",
        },
        KeySourceTestCase {
            description: "a named profile without keys fails, naming what isn't read",
            profile: None,
            credentials_file: "credentials",
            variables: &[("AWS_PROFILE", "prod")],
            expected_text: "has no aws_access_key_id and aws_secret_access_key for AWS profile prod; SQLLoad reads only keys from that file (SSO",
        },
        KeySourceTestCase {
            description: "a named profile whose file is missing fails",
            profile: Some("temporary"),
            credentials_file: "missing",
            variables: &[],
            expected_text: "for AWS profile temporary",
        },
        KeySourceTestCase {
            description: "only a key ID in the environment is not a pair, so the file's keys are used",
            profile: None,
            credentials_file: "credentials",
            variables: &[("AWS_ACCESS_KEY_ID", "AKIAENV")],
            expected_text: "credentials, profile default",
        },
        KeySourceTestCase {
            description: "no file and no named profile go on down the chain",
            profile: None,
            credentials_file: "missing",
            variables: &[],
            expected_text: "web identity or the instance or container role",
        },
    ];
    for test_case in &test_cases {
        let file = directory
            .path()
            .join(test_case.credentials_file)
            .display()
            .to_string();
        let variable = |name: &str| {
            test_case
                .variables
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_owned())
                .or_else(|| (name == "AWS_SHARED_CREDENTIALS_FILE").then(|| file.clone()))
        };
        let found = key_source(test_case.profile, &variable)
            .map(|source| source.describe())
            .unwrap_or_else(|error| error);
        assert!(
            found.contains(test_case.expected_text),
            "{}: {found}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_aws_variables_and_the_chains_keys_when_building_an_s3_client_then_only_the_chains_keys_reach_it()
 {
    let test_cases = [
        KeyedBuilderTestCase {
            description: "a leftover session token is dropped when the keys come from the file",
            variables: &[
                ("AWS_SESSION_TOKEN", "STALE"),
                ("AWS_ACCESS_KEY_ID", "AKIAENV"),
                ("AWS_REGION", "eu-west-2"),
            ],
            keys: Some(("AKIAFILE", None)),
            expected_config: [Some("AKIAFILE"), None, Some("eu-west-2")],
        },
        KeyedBuilderTestCase {
            description: "the chosen keys keep their own session token",
            variables: &[("AWS_TOKEN", "STALE")],
            keys: Some(("ASIAFILE", Some("fresh"))),
            expected_config: [Some("ASIAFILE"), Some("fresh"), None],
        },
        KeyedBuilderTestCase {
            description: "with no static keys, partial or empty ones in the environment are not used",
            variables: &[
                ("AWS_ACCESS_KEY_ID", "AKIAPARTIAL"),
                ("AWS_SECRET_ACCESS_KEY", ""),
                ("AWS_REGION", ""),
            ],
            keys: None,
            expected_config: [None, None, None],
        },
    ];
    for test_case in &test_cases {
        let variables = test_case
            .variables
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()));
        let keys = test_case.keys.map(|(key_id, token)| AwsCredentials {
            key_id: key_id.to_owned(),
            secret: Secret("secret".to_owned()),
            session_token: token.map(|token| Secret(token.to_owned())),
        });
        let builder = keyed_builder(variables, keys);
        let found = [
            AmazonS3ConfigKey::AccessKeyId,
            AmazonS3ConfigKey::Token,
            AmazonS3ConfigKey::Region,
        ]
        .map(|key| builder.get_config_value(&key));
        assert_eq!(
            found.each_ref().map(Option::as_deref),
            test_case.expected_config,
            "{}",
            test_case.description
        );
    }
}
