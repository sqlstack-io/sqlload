use crate::column_types::models::ColumnType;
use crate::file_store::models::StoredFile;
use crate::files_source::main::select_files::select_files;
use crate::files_source::models::{DataFile, FileSelection, LoadedFile};
use crate::files_source::tests::test_types::SelectFilesTestCase;

const KEYS: [(&str, &str); 3] = [
    ("day=2026-11-03/a.parquet", "2026-11-03"),
    ("day=2026-11-04/a.parquet", "2026-11-04"),
    ("day=2026-11-05/a.parquet", "2026-11-05"),
];

#[test]
fn given_listed_files_when_selecting_then_loaded_files_and_those_outside_the_window_are_skipped() {
    let test_cases = [
        SelectFilesTestCase {
            description: "without a cursor every file not loaded yet is read",
            loaded: &[("/data/day=2026-11-03/a.parquet", "1-0")],
            cursor_type: None,
            bounds: (None, None),
            expected_keys: Ok(&["day=2026-11-04/a.parquet", "day=2026-11-05/a.parquet"]),
        },
        SelectFilesTestCase {
            description: "a file loaded before versions were recorded is skipped",
            loaded: &[("/data/day=2026-11-03/a.parquet", "")],
            cursor_type: None,
            bounds: (None, None),
            expected_keys: Ok(&["day=2026-11-04/a.parquet", "day=2026-11-05/a.parquet"]),
        },
        SelectFilesTestCase {
            description: "a loaded file whose version changed is an error",
            loaded: &[("/data/day=2026-11-04/a.parquet", "9-9")],
            cursor_type: None,
            bounds: (None, None),
            expected_keys: Err("was rewritten in place"),
        },
        SelectFilesTestCase {
            description: "a date cursor reads from <= value < to",
            loaded: &[],
            cursor_type: Some(ColumnType::Date),
            bounds: (Some("2026-11-04"), Some("2026-11-05")),
            expected_keys: Ok(&["day=2026-11-04/a.parquet"]),
        },
        SelectFilesTestCase {
            description: "a text cursor compares as text",
            loaded: &[],
            cursor_type: Some(ColumnType::Text),
            bounds: (Some("2026-11-05"), None),
            expected_keys: Ok(&["day=2026-11-05/a.parquet"]),
        },
        SelectFilesTestCase {
            description: "an int cursor needs integer values",
            loaded: &[],
            cursor_type: Some(ColumnType::Int),
            bounds: (Some("3"), None),
            expected_keys: Err("is not an integer"),
        },
        SelectFilesTestCase {
            description: "a timestamp partition cannot be a cursor",
            loaded: &[],
            cursor_type: Some(ColumnType::TimestampTz),
            bounds: (Some("2026-11-04 00:00:00+00"), None),
            expected_keys: Err("must be an int, date or text"),
        },
    ];
    let files: Vec<DataFile> = KEYS
        .iter()
        .map(|(key, day)| DataFile {
            file: StoredFile {
                path: object_store::path::Path::from(*key),
                relative_key: (*key).to_owned(),
                size: 1,
                etag: None,
                modified_ms: 0,
            },
            partitions: vec![("day".to_owned(), (*day).to_owned())],
        })
        .collect();
    for test_case in test_cases {
        let loaded: Vec<LoadedFile> = test_case
            .loaded
            .iter()
            .map(|(name, version)| LoadedFile {
                name: (*name).to_owned(),
                version: (*version).to_owned(),
            })
            .collect();
        let selection = FileSelection {
            files: &files,
            location: "/data/",
            loaded: &loaded,
            cursor: test_case
                .cursor_type
                .as_ref()
                .map(|column_type| ("day", column_type)),
            from: test_case.bounds.0,
            to: test_case.bounds.1,
        };
        let chosen: Result<Vec<String>, String> = select_files(&selection).map(|chosen| {
            chosen
                .into_iter()
                .map(|data| data.file.relative_key)
                .collect()
        });
        let expected_keys: Option<Vec<String>> = test_case
            .expected_keys
            .ok()
            .map(|keys| keys.iter().map(|key| (*key).to_owned()).collect());
        assert_eq!(
            chosen.as_ref().ok(),
            expected_keys.as_ref(),
            "{}",
            test_case.description
        );
        assert!(
            chosen
                .err()
                .unwrap_or_default()
                .contains(test_case.expected_keys.err().unwrap_or_default()),
            "{}",
            test_case.description
        );
    }
}
