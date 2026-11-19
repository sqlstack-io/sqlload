use std::sync::Arc;

use crate::adapters::databricks::main::stage::_writer_accepts::writer_accepts;
use crate::adapters::databricks::models::DatabricksDestination;
use crate::adapters::databricks::tests::test_types::WriterAcceptsTestCase;
use crate::auth::models::Secret;
use crate::databricks_api::models::DatabricksClient;
use crate::http::main::install_tls_provider::install_tls_provider;
use crate::routing::models::Store;

#[test]
fn given_external_locations_when_databricks_says_what_it_reads_itself_then_only_s3_ones_count() {
    let test_cases = [
        WriterAcceptsTestCase {
            description: "an S3 location is read where it lies",
            locations: &["s3://lake/raw/"],
            expected_pulls_from: &["s3://lake/raw/"],
        },
        WriterAcceptsTestCase {
            description: "a GCS location on Databricks on Google Cloud is left to SQLLoad, which checks what COPY INTO would change only for S3",
            locations: &["gs://lake/raw/", "s3://lake/raw/"],
            expected_pulls_from: &["s3://lake/raw/"],
        },
    ];
    install_tls_provider();
    for test_case in test_cases {
        let destination = DatabricksDestination {
            api: DatabricksClient::new(
                reqwest::Client::new(),
                "example.cloud.databricks.com",
                "w1",
                ("id", Secret(String::new())),
            ),
            catalog: "main".to_owned(),
            volume: "/Volumes/main/raw/stage".to_owned(),
            ddl: Arc::default(),
            external_locations: Arc::new(tokio::sync::OnceCell::new_with(Some(
                test_case
                    .locations
                    .iter()
                    .map(|url| (*url).to_owned())
                    .collect(),
            ))),
        };
        assert_eq!(
            writer_accepts(&destination).pulls_from,
            std::iter::once(Store::InternalStage("/Volumes/main/raw/stage".to_owned()))
                .chain(
                    test_case
                        .expected_pulls_from
                        .iter()
                        .map(|url| Store::ObjectStore((*url).to_owned()))
                )
                .collect::<Vec<Store>>(),
            "{}",
            test_case.description
        );
    }
}
