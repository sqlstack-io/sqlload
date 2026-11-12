use crate::store_paths::_helpers::local_root::{strip_folders, windows_file_url_path};
use crate::store_paths::tests::helpers::root_of;
use crate::store_paths::tests::test_types::{
    FileUrlPathTestCase, LocalRootTestCase, StripFoldersTestCase,
};

#[test]
fn given_local_paths_when_finding_their_root_then_drives_and_shares_are_roots_only_on_windows() {
    let test_cases = [
        LocalRootTestCase {
            description: "a Unix absolute path",
            path: "/data/exports",
            expected_windows: Some("/|data/exports"),
            expected_unix: Some("/|data/exports"),
        },
        LocalRootTestCase {
            description: "a drive with backslashes, lower-case letter",
            path: r"c:\Data\exports",
            expected_windows: Some("C:/|Data/exports"),
            expected_unix: None,
        },
        LocalRootTestCase {
            description: "a drive as a file:/// URL gives it",
            path: "/D:/lake",
            expected_windows: Some("D:/|lake"),
            expected_unix: Some("/|D:/lake"),
        },
        LocalRootTestCase {
            description: "a UNC share",
            path: r"\\nas01\exports\daily\orders",
            expected_windows: Some("//nas01/exports/|daily/orders"),
            expected_unix: None,
        },
        LocalRootTestCase {
            description: "a UNC share's root alone",
            path: r"\\nas01\exports",
            expected_windows: Some("//nas01/exports/|"),
            expected_unix: None,
        },
        LocalRootTestCase {
            description: "an extended-length drive path",
            path: r"\\?\C:\very\long",
            expected_windows: Some("C:/|very/long"),
            expected_unix: None,
        },
        LocalRootTestCase {
            description: "an extended-length UNC path",
            path: r"\\?\UNC\nas01\exports\x",
            expected_windows: Some("//nas01/exports/|x"),
            expected_unix: None,
        },
        LocalRootTestCase {
            description: "a server without a share is no root",
            path: r"\\nas01",
            expected_windows: None,
            expected_unix: None,
        },
        LocalRootTestCase {
            description: "a drive-relative path is relative",
            path: "C:data",
            expected_windows: None,
            expected_unix: None,
        },
        LocalRootTestCase {
            description: "a relative path",
            path: r"exports\daily",
            expected_windows: None,
            expected_unix: None,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            root_of(test_case.path, true).as_deref(),
            test_case.expected_windows,
            "{} on Windows",
            test_case.description
        );
        assert_eq!(
            root_of(test_case.path, false).as_deref(),
            test_case.expected_unix,
            "{} on Unix",
            test_case.description
        );
    }
}

#[test]
fn given_file_urls_when_reading_their_path_then_windows_drives_and_shares_are_found() {
    let test_cases = [
        FileUrlPathTestCase {
            description: "an absolute path",
            rest: "/data/x",
            expected_windows: "/data/x",
            expected_unix: "/data/x",
        },
        FileUrlPathTestCase {
            description: "a drive",
            rest: "/C:/data/x",
            expected_windows: "C:/data/x",
            expected_unix: "/C:/data/x",
        },
        FileUrlPathTestCase {
            description: "a drive on localhost",
            rest: "LocalHost/C:/data/x",
            expected_windows: "C:/data/x",
            expected_unix: "LocalHost/C:/data/x",
        },
        FileUrlPathTestCase {
            description: "a share on localhost",
            rest: "localhost/exports/x",
            expected_windows: "//localhost/exports/x",
            expected_unix: "localhost/exports/x",
        },
        FileUrlPathTestCase {
            description: "a share",
            rest: "nas01/exports/x",
            expected_windows: "//nas01/exports/x",
            expected_unix: "nas01/exports/x",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            windows_file_url_path(test_case.rest, true),
            test_case.expected_windows,
            "{} on Windows",
            test_case.description
        );
        assert_eq!(
            windows_file_url_path(test_case.rest, false),
            test_case.expected_unix,
            "{} on Unix",
            test_case.description
        );
    }
}

#[test]
fn given_a_folder_prefix_when_stripping_a_connections_folders_then_windows_ignores_case() {
    let test_cases = [
        StripFoldersTestCase {
            description: "the same folders",
            prefix: "lake/scores/",
            folders: "lake/",
            expected_windows: Some("scores/"),
            expected_unix: Some("scores/"),
        },
        StripFoldersTestCase {
            description: "folders differing in case",
            prefix: "Lake/scores/",
            folders: "lake/",
            expected_windows: Some("scores/"),
            expected_unix: None,
        },
        StripFoldersTestCase {
            description: "a folder that only starts with the connection's",
            prefix: "lakehouse/scores/",
            folders: "lake/",
            expected_windows: None,
            expected_unix: None,
        },
        StripFoldersTestCase {
            description: "a shorter prefix",
            prefix: "la/",
            folders: "lake/",
            expected_windows: None,
            expected_unix: None,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            strip_folders(test_case.prefix, test_case.folders, true),
            test_case.expected_windows,
            "{} on Windows",
            test_case.description
        );
        assert_eq!(
            strip_folders(test_case.prefix, test_case.folders, false),
            test_case.expected_unix,
            "{} on Unix",
            test_case.description
        );
    }
}
