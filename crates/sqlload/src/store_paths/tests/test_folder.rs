use crate::store_paths::_helpers::folder::folder;
use crate::store_paths::tests::test_types::FolderTestCase;

#[test]
fn given_path_parts_when_joining_then_one_clean_folder_comes_out() {
    let test_cases = [
        FolderTestCase {
            description: "parts are joined with single slashes and end in a slash",
            parts: &["/models//scores", "date=2026-11-04"],
            expected_folder: Ok("models/scores/date=2026-11-04/"),
        },
        FolderTestCase {
            description: "a prefix is always a folder",
            parts: &["ratings"],
            expected_folder: Ok("ratings/"),
        },
        FolderTestCase {
            description: "dot segments are dropped",
            parts: &["./a/./b/"],
            expected_folder: Ok("a/b/"),
        },
        FolderTestCase {
            description: "nothing joins to the root",
            parts: &["", "/"],
            expected_folder: Ok(""),
        },
        FolderTestCase {
            description: "parent segments are rejected",
            parts: &["a/../b"],
            expected_folder: Err("\"..\" segment"),
        },
        FolderTestCase {
            description: "control characters are rejected",
            parts: &["a\nb"],
            expected_folder: Err("control character"),
        },
    ];
    for test_case in test_cases {
        let result = folder(test_case.parts).map_err(|error| error.to_string());
        assert_eq!(
            result.as_deref().ok(),
            test_case.expected_folder.ok(),
            "{}",
            test_case.description
        );
        assert!(
            result
                .err()
                .unwrap_or_default()
                .contains(test_case.expected_folder.err().unwrap_or_default()),
            "{}",
            test_case.description
        );
    }
}
