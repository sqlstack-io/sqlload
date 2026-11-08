use crate::file_store::main::s3_keys::s3_keys;
use crate::stage_api::models::AwsCredentials;

/// AWS keys from `AWS_ACCESS_KEY_ID` and friends, else the shared credentials file (`AWS_SHARED_CREDENTIALS_FILE`, default `~/.aws/credentials`) under `AWS_PROFILE` or `default`, read as the loader reads them.
pub(crate) fn load_credentials() -> Result<AwsCredentials, String> {
    s3_keys(None)?.ok_or_else(|| {
        "no AWS credentials: set AWS_ACCESS_KEY_ID, or keys in the shared credentials file (AWS_SHARED_CREDENTIALS_FILE, default ~/.aws/credentials)".to_owned()
    })
}
