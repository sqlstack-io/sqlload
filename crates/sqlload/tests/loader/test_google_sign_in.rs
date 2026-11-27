use crate::google_helpers::{
    IMPERSONATED, azure, denied, executable, gcloud, impersonated_user, missing, named, no_file,
    nothing, nowhere, refused_refresh, refused_subject, short_tokens, sign_in, token_file,
    user_file, workforce,
};
use crate::test_types::SignInTestCase;

const BASE: SignInTestCase = SignInTestCase {
    description: "",
    file: no_file,
    place: nowhere,
    metadata_host: None,
    prepare: nothing,
    queries: 2,
    expected_sign_ins: &[],
    expected_error: "",
};

#[test]
fn given_google_application_default_credentials_when_bigquery_signs_in_then_each_kind_gets_and_renews_its_token()
-> Result<(), String> {
    let test_cases = [
        SignInTestCase {
            description: "a gcloud login is renewed with its refresh token and client, once while the token lasts",
            file: user_file,
            place: gcloud,
            expected_sign_ins: &["refresh client_id=gcloud-client"],
            ..BASE
        },
        SignInTestCase {
            description: "a gcloud login's token that lasts under the renewal margin is renewed before every request",
            file: user_file,
            place: gcloud,
            prepare: short_tokens,
            expected_sign_ins: &[
                "refresh client_id=gcloud-client",
                "refresh client_id=gcloud-client",
            ],
            ..BASE
        },
        SignInTestCase {
            description: "a refresh token Google refuses fails naming the step and Google's reason, never falling through",
            file: user_file,
            place: gcloud,
            prepare: refused_refresh,
            expected_sign_ins: &["refresh client_id=gcloud-client"],
            expected_error: "renewing the gcloud login's token (run `gcloud auth application-default login` again if Google refuses it) refused (HTTP 400): invalid_grant: Token has been expired or revoked.",
            ..BASE
        },
        SignInTestCase {
            description: "without a file, the metadata server's token for BigQuery's scope",
            expected_sign_ins: &["metadata scopes=https://www.googleapis.com/auth/bigquery"],
            ..BASE
        },
        SignInTestCase {
            description: "an impersonated_service_account file: the login's token gets a one-hour token for the account",
            file: impersonated_user,
            place: named,
            expected_sign_ins: &["refresh client_id=gcloud-client", IMPERSONATED],
            ..BASE
        },
        SignInTestCase {
            description: "an impersonated token is renewed before it expires, with a fresh token of the identity signing in",
            file: impersonated_user,
            place: named,
            prepare: short_tokens,
            expected_sign_ins: &[
                "refresh client_id=gcloud-client",
                IMPERSONATED,
                "refresh client_id=gcloud-client",
                IMPERSONATED,
            ],
            ..BASE
        },
        SignInTestCase {
            description: "an account the identity may not impersonate fails naming the account, the role it needs and Google's reason",
            file: impersonated_user,
            place: named,
            prepare: denied,
            expected_sign_ins: &["refresh client_id=gcloud-client", IMPERSONATED],
            expected_error: "IAM Credentials' generateAccessToken, impersonating loader@fake-project.iam.gserviceaccount.com (the identity signing in needs roles/iam.serviceAccountTokenCreator on it) refused (HTTP 403): PERMISSION_DENIED: Permission 'iam.serviceAccounts.getAccessToken' denied",
            ..BASE
        },
        SignInTestCase {
            description: "a file-sourced federated token is exchanged at STS, then impersonates the account the file names",
            file: token_file,
            place: named,
            expected_sign_ins: &[
                "sts audience=//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/sqlload-pool/providers/sqlload-provider type=urn:ietf:params:oauth:token-type:jwt scope=https://www.googleapis.com/auth/cloud-platform subject=file-subject-token",
                IMPERSONATED,
            ],
            ..BASE
        },
        SignInTestCase {
            description: "Azure: the managed identity's token from IMDS (api-version, resource, Metadata: True), exchanged at STS",
            file: azure,
            place: named,
            queries: 1,
            expected_sign_ins: &[
                "azure api-version=2018-02-01 resource=api://sqlload-provider metadata=True",
                "sts audience=//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/sqlload-pool/providers/sqlload-provider type=urn:ietf:params:oauth:token-type:jwt scope=https://www.googleapis.com/auth/cloud-platform subject=azure-subject-",
                IMPERSONATED,
            ],
            ..BASE
        },
        SignInTestCase {
            description: "a subject token STS refuses fails naming the provider and STS's reason",
            file: refused_subject,
            place: named,
            expected_sign_ins: &["sts "],
            expected_error: "Google STS's token exchange, for workload identity provider //iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/sqlload-pool/providers/sqlload-provider refused (HTTP 400): invalid_grant: The subject token is invalid.",
            ..BASE
        },
        SignInTestCase {
            description: "a command for the subject token runs only with GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES=1, as Google's libraries require",
            file: executable,
            place: named,
            expected_error: "needs GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES=1",
            ..BASE
        },
        SignInTestCase {
            description: "a workforce pool file is refused by name",
            file: workforce,
            place: named,
            expected_error: "signs in through a workforce identity pool, which SQLLoad does not support",
            ..BASE
        },
        SignInTestCase {
            description: "GOOGLE_APPLICATION_CREDENTIALS naming no file fails rather than signing in as the machine",
            place: missing,
            expected_error: "which cannot be read",
            ..BASE
        },
        SignInTestCase {
            description: "off Google Cloud, with no metadata server, the error says how to sign in instead",
            metadata_host: Some("127.0.0.1:9"),
            queries: 1,
            expected_error: "the metadata server (127.0.0.1:9), for the machine's service account; off Google Cloud, sign in with service_account_key, GOOGLE_APPLICATION_CREDENTIALS or `gcloud auth application-default login`",
            ..BASE
        },
    ];
    for test_case in &test_cases {
        let (seen, sign_ins) = sign_in(test_case)?;
        let starts = sign_ins.len() == test_case.expected_sign_ins.len()
            && sign_ins
                .iter()
                .zip(test_case.expected_sign_ins)
                .all(|(line, start)| line.starts_with(start));
        assert!(starts, "{}: sign-ins {sign_ins:#?}", test_case.description);
        let error = seen.err().unwrap_or_default();
        assert_eq!(
            error.is_empty(),
            test_case.expected_error.is_empty(),
            "{}: {error}",
            test_case.description
        );
        assert!(
            error.contains(test_case.expected_error),
            "{}: {error}",
            test_case.description
        );
    }
    Ok(())
}
