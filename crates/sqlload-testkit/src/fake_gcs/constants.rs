/// fake-gcs-server as object_store tests against it, with the XML API's object requests; pinned by digest, as it has only a `latest` tag.
pub const IMAGE: &str = "tustvold/fake-gcs-server";
pub const TAG: &str =
    "latest@sha256:dcd3aeacc07c731f1336e90c2889be2af8626ae993ee1fe2c0ba042ebbeb5a04";
pub const PORT: u16 = 4443;
/// The bucket every test uses, made when the server starts.
pub const BUCKET: &str = "example-bucket-test";
/// A service-account key that turns off OAuth, so the client sends no token the fake would have to check.
pub const SERVICE_ACCOUNT_KEY: &str = r#"{"private_key": "", "private_key_id": "", "client_email": "test@example.com", "disable_oauth": true}"#;
