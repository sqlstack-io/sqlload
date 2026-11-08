use crate::stage_api::_helpers::stage_response::{parse_stage_downloads, parse_stage_info};
use crate::stage_api::tests::test_types::{
    StageDownloadsTestCase, StageResponseErrorTestCase, StageResponseTestCase,
};

#[test]
fn given_an_s3_put_response_when_parsing_then_the_bucket_location_is_read() -> Result<(), String> {
    let test_cases = [StageResponseTestCase {
        description: "an S3 stage gives the virtual-hosted bucket and the stage prefix",
        response: r#"{"stageInfo":{"locationType":"S3","location":"example-bucket/stages/abc","region":"us-west-2","endPoint":"","creds":{"AWS_KEY_ID":"K","AWS_SECRET_KEY":"S","AWS_TOKEN":"T"},"credExpiryTime":1000},"encryptionMaterial":{"queryStageMasterKey":"AAAAAAAAAAAAAAAAAAAAAA==","queryId":"q","smkId":42}}"#,
        expected_host: "example-bucket.s3.us-west-2.amazonaws.com",
        expected_path: "/stages/abc/2024-01-01/f.json.gz",
    }];
    for test_case in &test_cases {
        let data: serde_json::Value =
            serde_json::from_str(test_case.response).map_err(|error| error.to_string())?;
        let info = parse_stage_info(&data)?;
        let (host, path) = info.target.host_and_path("2024-01-01/f.json.gz");
        assert_eq!(host, test_case.expected_host, "{}", test_case.description);
        assert_eq!(path, test_case.expected_path, "{}", test_case.description);
    }
    Ok(())
}

#[test]
fn given_unsupported_put_responses_when_parsing_then_they_are_rejected() -> Result<(), String> {
    let test_cases = [
        StageResponseErrorTestCase {
            description: "Azure stages are not supported",
            response: r#"{"stageInfo":{"locationType":"AZURE"}}"#,
            expected_error: "only S3-backed",
        },
        StageResponseErrorTestCase {
            description: "stages without client-side encryption are rejected",
            response: r#"{"stageInfo":{"locationType":"S3","location":"b/p","region":"r","creds":{"AWS_KEY_ID":"K","AWS_SECRET_KEY":"S"}}}"#,
            expected_error: "not client-side encrypted",
        },
    ];
    for test_case in &test_cases {
        let data: serde_json::Value =
            serde_json::from_str(test_case.response).map_err(|error| error.to_string())?;
        let error = parse_stage_info(&data).expect_err(test_case.description);
        assert!(
            error.contains(test_case.expected_error),
            "{}: {error}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_a_get_response_when_parsing_then_each_file_keeps_its_own_key() -> Result<(), String> {
    let test_cases = [
        StageDownloadsTestCase {
            description: "each file has the key at its position",
            response: "{\"stageInfo\":{\"locationType\":\"S3\",\"location\":\"b/stages/u\",\"region\":\"eu-west-2\",\"creds\":{\"AWS_KEY_ID\":\"K\",\"AWS_SECRET_KEY\":\"S\"}},\"src_locations\":[\"stages/u/run/a.parquet\",\"stages/u/run/b.parquet\"],\"encryptionMaterial\":[{\"queryStageMasterKey\":\"AAAAAAAAAAAAAAAAAAAAAA==\",\"queryId\":\"qa\",\"smkId\":\"7\"},{\"queryStageMasterKey\":\"AAAAAAAAAAAAAAAAAAAAAA==\",\"queryId\":\"qb\",\"smkId\":\"7\"}]}",
            expected_files: Ok(vec![
                ("stages/u/run/a.parquet", "qa"),
                ("stages/u/run/b.parquet", "qb"),
            ]),
        },
        StageDownloadsTestCase {
            description: "one key serves every file",
            response: "{\"stageInfo\":{\"locationType\":\"S3\",\"location\":\"b/stages/u\",\"region\":\"eu-west-2\",\"creds\":{\"AWS_KEY_ID\":\"K\",\"AWS_SECRET_KEY\":\"S\"}},\"src_locations\":[\"stages/u/run/a.parquet\",\"stages/u/run/b.parquet\"],\"encryptionMaterial\":{\"queryStageMasterKey\":\"AAAAAAAAAAAAAAAAAAAAAA==\",\"queryId\":\"q\",\"smkId\":\"7\"}}",
            expected_files: Ok(vec![
                ("stages/u/run/a.parquet", "q"),
                ("stages/u/run/b.parquet", "q"),
            ]),
        },
        StageDownloadsTestCase {
            description: "a file without a key is refused rather than paired with another file's",
            response: "{\"stageInfo\":{\"locationType\":\"S3\",\"location\":\"b/stages/u\",\"region\":\"eu-west-2\",\"creds\":{\"AWS_KEY_ID\":\"K\",\"AWS_SECRET_KEY\":\"S\"}},\"src_locations\":[\"stages/u/run/a.parquet\",\"stages/u/run/b.parquet\"],\"encryptionMaterial\":[{\"queryStageMasterKey\":\"AAAAAAAAAAAAAAAAAAAAAA==\",\"queryId\":\"qa\",\"smkId\":\"7\"}]}",
            expected_files: Err("the GET response names 2 files but has 1 keys"),
        },
    ];
    for test_case in test_cases {
        let data: serde_json::Value =
            serde_json::from_str(test_case.response).map_err(|error| error.to_string())?;
        let files = parse_stage_downloads(&data).map(|downloads| {
            downloads
                .files
                .into_iter()
                .map(|file| (file.key, file.encryption.query_id))
                .collect::<Vec<(String, String)>>()
        });
        assert_eq!(
            files,
            test_case
                .expected_files
                .map(|files| {
                    files
                        .into_iter()
                        .map(|(key, query)| (key.to_owned(), query.to_owned()))
                        .collect()
                })
                .map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
    Ok(())
}
