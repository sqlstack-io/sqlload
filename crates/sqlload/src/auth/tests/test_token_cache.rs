use crate::auth::_helpers::oauth::now_secs;
use crate::auth::_helpers::token_cache::{read_cached, write_cached};
use crate::auth::tests::helpers::{test_client, tokens};
use crate::auth::tests::test_types::TokenCacheTestCase;

#[test]
fn given_sign_ins_of_several_users_when_caching_them_then_each_is_kept_in_a_file_only_its_owner_can_read()
-> Result<(), String> {
    let test_cases = [TokenCacheTestCase {
        description: "two users, the first cached twice",
        users: &["alice", "bob", "alice"],
        expected_tokens: &["access-alice-2", "access-bob-1"],
        expected_mode: 0o600,
    }];
    for test_case in &test_cases {
        let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
        let file = folder.path().join("nested").join("tokens.json");
        for (index, user) in test_case.users.iter().enumerate() {
            let client = test_client("https://example.snowflakecomputing.com", &file, user)?;
            let written = test_case.users[..=index]
                .iter()
                .filter(|earlier| *earlier == user)
                .count();
            write_cached(
                &client,
                &tokens(now_secs(), &format!("access-{user}-{written}"), 600, None),
            )?;
        }
        let mut found = Vec::new();
        for user in ["alice", "bob"] {
            let client = test_client("https://example.snowflakecomputing.com", &file, user)?;
            found.push(
                read_cached(&client)
                    .map(|cached| cached.access_token.expose().to_owned())
                    .unwrap_or_default(),
            );
        }
        assert_eq!(
            found, test_case.expected_tokens,
            "{}",
            test_case.description
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&file)
                .map_err(|error| error.to_string())?
                .permissions()
                .mode();
            assert_eq!(
                mode & 0o777,
                test_case.expected_mode,
                "{}",
                test_case.description
            );
        }
    }
    Ok(())
}
