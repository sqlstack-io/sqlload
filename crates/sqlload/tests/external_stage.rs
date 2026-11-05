//! Live test of decision 57: the same files loaded by Snowflake itself and through SQLLoad give the same rows and the same failures. It needs a Snowflake account and an S3 bucket, and is skipped without `SQLLOAD_PARITY_ACCOUNT`.

#[path = "external_stage/test_parity.rs"]
mod test_parity;
#[path = "external_stage/test_types.rs"]
mod test_types;
