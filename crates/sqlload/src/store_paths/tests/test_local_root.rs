use crate::store_paths::_helpers::local_root::{
    local_root, windows_file_url_path, windows_separators,
};

/// How a local path reads on Windows, and on Unix, where only `/` starts an absolute path.
struct LocalRootTestCase {
    description: &'static str,
    path: &'static str,
    /// Root and folders on Windows; `None` for a relative path.
    expected_windows: Option<(&'static str, &'static str)>,
    expected_unix: Option<(&'static str, &'static str)>,
}

#[test]
fn given_local_paths_when_finding_their_root_then_drives_and_shares_are_roots_only_on_windows() {
    let test_cases = [
        LocalRootTestCase {
            description: "a Unix absolute path",
            path: "/data/exports",
            expected_windows: Some(("/", "data/exports")),
            expected_unix: Some(("/", "data/exports")),
        },
        LocalRootTestCase {
            description: "a drive with backslashes, lower-case letter",
            path: r"c:\Data\exports",
            expected_windows: Some(("C:/", "Data/exports")),
            expected_unix: None,
        },
        LocalRootTestCase {
            description: "a drive as a file:/// URL gives it",
            path: "/D:/lake",
            expected_windows: Some(("D:/", "lake")),
            expected_unix: Some(("/", "D:/lake")),
        },
        LocalRootTestCase {
            description: "a UNC share",
            path: r"\\nas01\exports\daily\orders",
            expected_windows: Some(("//nas01/exports/", "daily/orders")),
            expected_unix: None,
        },
        LocalRootTestCase {
            description: "a UNC share's root alone",
            path: r"\\nas01\exports",
            expected_windows: Some(("//nas01/exports/", "")),
            expected_unix: None,
        },
        LocalRootTestCase {
            description: "an extended-length drive path",
            path: r"\\?\C:\very\long",
            expected_windows: Some(("C:/", "very/long")),
            expected_unix: None,
        },
        LocalRootTestCase {
            description: "an extended-length UNC path",
            path: r"\\?\UNC\nas01\exports\x",
            expected_windows: Some(("//nas01/exports/", "x")),
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
        for (windows, expected) in [
            (true, test_case.expected_windows),
            (false, test_case.expected_unix),
        ] {
            let separated = windows_separators(test_case.path, windows);
            let found = local_root(&separated, windows);
            let found = found.as_ref().map(|(root, rest)| (root.as_str(), *rest));
            assert_eq!(
                found, expected,
                "{} (windows: {windows})",
                test_case.description
            );
        }
    }
}

#[test]
fn given_file_urls_when_reading_their_path_then_windows_drives_and_shares_are_found() {
    let test_cases = [
        ("/data/x", "/data/x", "/data/x"),
        ("/C:/data/x", "C:/data/x", "/C:/data/x"),
        ("nas01/exports/x", "//nas01/exports/x", "nas01/exports/x"),
    ];
    for (rest, expected_windows, expected_unix) in test_cases {
        assert_eq!(
            windows_file_url_path(rest, true),
            expected_windows,
            "{rest}"
        );
        assert_eq!(windows_file_url_path(rest, false), expected_unix, "{rest}");
    }
}
