//! Run a statement that has DuckDB read files itself, giving it access to the files' bucket first.

use crate::adapters::duckdb::_helpers::blocking::with_connection;
use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::DuckDbDestination;
use crate::store_paths::models::StoreKind;

/// Run `insert` and return the rows it loaded. For S3 files it first loads `httpfs` (installing it on first use) and sets a temporary secret scoped to the bucket and named after it (hex, so two buckets never share a name), in the connection's region, else `AWS_REGION` / `AWS_DEFAULT_REGION`, else us-east-1 as SQLLoad's reader does: the `AWS_*` keys when the environment has them, which are passed as parameters and never appear in SQL, otherwise the AWS credential chain.
pub(in crate::adapters) async fn pull_files(
    destination: &DuckDbDestination,
    store: &StoreKind,
    insert: String,
) -> Result<u64, DuckDbError> {
    let bucket = match store {
        StoreKind::S3 { bucket, region, .. } => Some((bucket.clone(), region.clone())),
        StoreKind::Filesystem { .. } => None,
    };
    let keys: Option<[String; 3]> = match (
        std::env::var("AWS_ACCESS_KEY_ID"),
        std::env::var("AWS_SECRET_ACCESS_KEY"),
    ) {
        (Ok(key), Ok(secret)) => Some([
            key,
            secret,
            std::env::var("AWS_SESSION_TOKEN").unwrap_or_default(),
        ]),
        _ => None,
    };
    let region = std::env::var("AWS_REGION")
        .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| "us-east-1".to_owned());
    with_connection(destination, move |connection| {
        if let Some((bucket, bucket_region)) = bucket {
            connection.execute_batch("INSTALL httpfs; LOAD httpfs;")?;
            let scope = format!("s3://{bucket}/");
            let name: String = bucket.bytes().map(|byte| format!("{byte:02x}")).collect();
            let region = bucket_region.unwrap_or(region);
            match keys {
                Some([key, secret, token]) => connection.execute(
                    &format!("CREATE OR REPLACE TEMPORARY SECRET sqlload_s3_{name} (TYPE s3, KEY_ID ?, SECRET ?, SESSION_TOKEN ?, REGION ?, SCOPE ?)"),
                    [key, secret, token, region, scope],
                ),
                None => {
                    connection.execute_batch("INSTALL aws; LOAD aws;")?;
                    connection.execute(
                        &format!("CREATE OR REPLACE TEMPORARY SECRET sqlload_s3_{name} (TYPE s3, PROVIDER credential_chain, REGION ?, SCOPE ?)"),
                        [region, scope],
                    )
                }
            }?;
        }
        Ok(connection.execute(&insert, [])? as u64)
    })
    .await
}
