use std::collections::BTreeMap;

use crate::stage_api::_helpers::sigv4::{SignInput, sign_v4};
use crate::stage_api::tests::test_types::SignTestCase;

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

#[test]
fn given_aws_documented_requests_when_signing_then_signatures_match() {
    let test_cases = [SignTestCase {
        description: "the GET Object example from the AWS SigV4 documentation",
        method: "GET",
        uri: "/test.txt",
        headers: &[
            ("host", "examplebucket.s3.amazonaws.com"),
            ("range", "bytes=0-9"),
            ("x-amz-content-sha256", EMPTY_SHA256),
            ("x-amz-date", "20130524T000000Z"),
        ],
        payload_hash: EMPTY_SHA256,
        expected_signature: "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41",
    }];
    for test_case in &test_cases {
        let headers: BTreeMap<String, String> = test_case
            .headers
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        let authorization = sign_v4(&SignInput {
            method: test_case.method,
            canonical_uri: test_case.uri,
            canonical_query: "",
            headers: &headers,
            payload_hash: test_case.payload_hash,
            region: "us-east-1",
            amz_date: "20130524T000000Z",
            key_id: "AKIAIOSFODNN7EXAMPLE",
            secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        });
        let signature = authorization
            .rsplit("Signature=")
            .next()
            .unwrap_or_default();
        assert_eq!(
            signature, test_case.expected_signature,
            "{}",
            test_case.description
        );
    }
}
