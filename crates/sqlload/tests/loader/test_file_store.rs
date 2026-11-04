use bytes::Bytes;
use sqlload::file_store::main::list_files::list_files;
use sqlload::file_store::main::open_folder::open_folder;
use sqlload::file_store::main::read_file::read_file;
use sqlload::file_store::main::write_file::write_file;
use sqlload::store_paths::models::{Location, StoreKind};
use sqlload_testkit::seaweedfs::constants::REGION;
use sqlload_testkit::seaweedfs::main::create_bucket::create_bucket;
use sqlload_testkit::seaweedfs::main::start_seaweedfs::start_seaweedfs;

use crate::helpers::run_async;
use crate::test_types::FileStoreTestCase;

#[test]
fn given_a_store_when_writing_and_listing_files_then_data_files_round_trip_and_hidden_ones_are_skipped()
-> Result<(), String> {
    let test_cases = [
        FileStoreTestCase {
            description: "S3 through SeaweedFS",
            store: "s3",
        },
        FileStoreTestCase {
            description: "a local directory",
            store: "filesystem",
        },
    ];
    run_async(async {
        let server = start_seaweedfs().await?;
        create_bucket(&server, "example-bucket-test").await?;
        let directory = std::env::temp_dir().join(format!("sqlload-store-{}", std::process::id()));
        for test_case in test_cases {
            let store = match test_case.store {
                "s3" => StoreKind::S3 {
                    bucket: "example-bucket-test".to_owned(),
                    region: Some(REGION.to_owned()),
                    endpoint: Some(server.endpoint.clone()),
                },
                _ => StoreKind::Filesystem {
                    root: directory.display().to_string(),
                },
            };
            let folder = open_folder(&Location {
                store,
                prefix: "models/scores/".to_owned(),
            })
            .map_err(|error| error.to_string())?;
            for key in [
                "date=2026-11-04/part-1.parquet",
                "date=2026-11-03/part-0.parquet",
                "_sqlload/runs/r.json",
                "date=2026-11-04/.part-2.tmp",
            ] {
                write_file(&folder, key, Bytes::from(key.to_owned()))
                    .await
                    .map_err(|error| error.to_string())?;
            }
            let files = list_files(&folder)
                .await
                .map_err(|error| error.to_string())?;
            let keys: Vec<&str> = files
                .iter()
                .map(|file| file.relative_key.as_str())
                .collect();
            assert_eq!(
                keys,
                [
                    "date=2026-11-03/part-0.parquet",
                    "date=2026-11-04/part-1.parquet"
                ],
                "{}",
                test_case.description
            );
            let body = read_file(&folder, "date=2026-11-04/part-1.parquet")
                .await
                .map_err(|error| error.to_string())?;
            assert_eq!(
                &body[..],
                b"date=2026-11-04/part-1.parquet",
                "{}",
                test_case.description
            );
            assert!(
                files.iter().all(|file| file.size > 0),
                "{}",
                test_case.description
            );
        }
        let _ = std::fs::remove_dir_all(&directory);
        Ok(())
    })
}
