//! An `s3`-mode configuration against a test broker and the fake service, which is also the bucket.

use sqlload::config::main::parse_config::parse_config;
use sqlload::config::models::Config;

use rsa::pkcs8::{EncodePrivateKey, LineEnding};

use crate::configs::constants::TEST_KEY_BITS;
use crate::configs::models::S3Settings;
use crate::fake_snowflake::constants::FAKE_BUCKET;

/// `s3` delivery under a throwaway key pair, so `load = "auto"` uses Snowpipe `insertFiles`.
pub fn s3_config(
    brokers: &str,
    base_url: &str,
    include: &str,
    settings: S3Settings<'_>,
) -> Result<Config, String> {
    let key_file = std::env::temp_dir().join(format!("sqlload-test-key-{}.p8", std::process::id()));
    let key = rsa::RsaPrivateKey::new(&mut rsa::rand_core::OsRng, TEST_KEY_BITS)
        .map_err(|error| error.to_string())?;
    let pem = key
        .to_pkcs8_pem(LineEnding::LF)
        .map_err(|error| error.to_string())?;
    std::fs::write(&key_file, pem.as_bytes()).map_err(|error| error.to_string())?;
    let text = format!(
        r#"
[source]
kind = "kafka"
brokers = "{brokers}"
include = ["{include}"]
start_from = "earliest"
discovery_interval = "2s"

[destination]
kind = "snowflake"
account = "test-account"
user = "LOADER"
auth = {{ method = "key_pair", private_key_file = "{key_file}" }}
database = "TEST_DB"
schema = "RAW"
base_url = "{base_url}"

[staging]
kind = "s3"
{staging}

[staging.s3]
bucket = "{FAKE_BUCKET}"
prefix = "exports/"
region = "us-east-1"
stage = "OTHER_DB.PUBLIC.LAKE"
endpoint = "{base_url}"
path_style = true
{s3}
{extra}
"#,
        key_file = key_file.display(),
        staging = settings.staging,
        s3 = settings.s3,
        extra = settings.extra
    );
    parse_config(&text, &|_| None).map_err(|error| error.to_string())
}
