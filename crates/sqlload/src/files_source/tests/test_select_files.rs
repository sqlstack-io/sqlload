use crate::column_types::models::ColumnType;
use crate::file_store::models::StoredFile;
use crate::files_source::main::select_files::select_files;
use crate::files_source::models::{DataFile, FileSelection, LoadedFile, PathValues};
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
            held: &[],
            removed: false,
            cursor_type: None,
            bounds: (None, None),
            expected_keys: Ok(&["day=2026-11-04/a.parquet", "day=2026-11-05/a.parquet"]),
        },
        SelectFilesTestCase {
            description: "a file loaded without its version is an error, as a rewrite in place since can't be told",
            loaded: &[("/data/day=2026-11-03/a.parquet", "")],
            held: &[],
            removed: false,
            cursor_type: None,
            bounds: (None, None),
            expected_keys: Err(
                "was loaded without its version (_sqlload_source_version is empty, as in rows loaded before SQLLoad recorded it), so whether it was rewritten in place since can't be told; load the table again with --reload",
            ),
        },
        SelectFilesTestCase {
            description: "a file loaded without its version and again at the version listed is skipped",
            loaded: &[
                ("/data/day=2026-11-03/a.parquet", ""),
                ("/data/day=2026-11-03/a.parquet", "1-0"),
            ],
            held: &[],
            removed: false,
            cursor_type: None,
            bounds: (None, None),
            expected_keys: Ok(&["day=2026-11-04/a.parquet", "day=2026-11-05/a.parquet"]),
        },
        SelectFilesTestCase {
            description: "a loaded file whose version changed is an error",
            loaded: &[("/data/day=2026-11-04/a.parquet", "9-9")],
            held: &[],
            removed: false,
            cursor_type: None,
            bounds: (None, None),
            expected_keys: Err("was rewritten in place"),
        },
        SelectFilesTestCase {
            description: "a name loaded at several versions is skipped when one is the version listed",
            loaded: &[
                ("/data/day=2026-11-04/a.parquet", "1-0"),
                ("/data/day=2026-11-04/a.parquet", "9-9"),
            ],
            held: &[],
            removed: false,
            cursor_type: None,
            bounds: (None, None),
            expected_keys: Ok(&["day=2026-11-03/a.parquet", "day=2026-11-05/a.parquet"]),
        },
        SelectFilesTestCase {
            description: "when loaded files leave the folder, a loaded name at another version is a new file",
            loaded: &[("/data/day=2026-11-04/a.parquet", "9-9")],
            held: &[],
            removed: true,
            cursor_type: None,
            bounds: (None, None),
            expected_keys: Ok(&[
                "day=2026-11-03/a.parquet",
                "day=2026-11-04/a.parquet",
                "day=2026-11-05/a.parquet",
            ]),
        },
        SelectFilesTestCase {
            description: "when loaded files leave the folder, one still at a loaded version is skipped",
            loaded: &[
                ("/data/day=2026-11-04/a.parquet", "9-9"),
                ("/data/day=2026-11-04/a.parquet", "1-0"),
            ],
            held: &[],
            removed: true,
            cursor_type: None,
            bounds: (None, None),
            expected_keys: Ok(&["day=2026-11-03/a.parquet", "day=2026-11-05/a.parquet"]),
        },
        SelectFilesTestCase {
            description: "a date cursor reads from <= value < to",
            loaded: &[],
            held: &[],
            removed: false,
            cursor_type: Some(ColumnType::Date),
            bounds: (Some("2026-11-04"), Some("2026-11-05")),
            expected_keys: Ok(&["day=2026-11-04/a.parquet"]),
        },
        SelectFilesTestCase {
            description: "under delete_insert, a file the table holds at another version below the window fails, naming the file and the fixes, rather than being skipped",
            loaded: &[],
            held: &[
                ("/data/day=2026-11-03/a.parquet", "9-9"),
                ("/data/day=2026-11-05/a.parquet", "1-0"),
            ],
            removed: false,
            cursor_type: Some(ColumnType::Date),
            bounds: (Some("2026-11-05"), None),
            expected_keys: Err(
                "/data/day=2026-11-03/a.parquet changed since it was loaded (version 9-9 is now 1-0), but its day 2026-11-03 is below this load's range, which starts at 2026-11-05, so delete_insert would skip it and keep its old rows. Fix: set replace_key to replace rows by a column the files hold (and drop cursor_column), load from it with --start-cursor 2026-11-03, or rebuild the table with --reload",
            ),
        },
        SelectFilesTestCase {
            description: "under delete_insert, a file held at its listed version below the window is skipped, one held at an unknown version inside it is read, and a changed one above it is outside the range asked for",
            loaded: &[],
            held: &[
                ("/data/day=2026-11-03/a.parquet", "1-0"),
                ("/data/day=2026-11-04/a.parquet", ""),
                ("/data/day=2026-11-05/a.parquet", "9-9"),
            ],
            removed: false,
            cursor_type: Some(ColumnType::Date),
            bounds: (Some("2026-11-04"), Some("2026-11-05")),
            expected_keys: Ok(&["day=2026-11-04/a.parquet"]),
        },
        SelectFilesTestCase {
            description: "under delete_insert, a file held only at an unknown version below the window fails, naming --reload, as whether it changed can't be told",
            loaded: &[],
            held: &[("/data/day=2026-11-03/a.parquet", "")],
            removed: false,
            cursor_type: Some(ColumnType::Date),
            bounds: (Some("2026-11-04"), None),
            expected_keys: Err(
                "/data/day=2026-11-03/a.parquet was loaded without its version (_sqlload_source_version is empty, as in rows loaded before SQLLoad recorded it), and its day 2026-11-03 is below this load's range, which starts at 2026-11-04, so whether it changed since can't be told and delete_insert would skip it. Fix: rebuild the table with --reload",
            ),
        },
        SelectFilesTestCase {
            description: "under delete_insert, a new file below the window fails when the table holds files of its partition, even when all of them have left the folder",
            loaded: &[],
            held: &[("/data/day=2026-11-03/gone.parquet", "1-0")],
            removed: false,
            cursor_type: Some(ColumnType::Date),
            bounds: (Some("2026-11-04"), None),
            expected_keys: Err(
                "/data/day=2026-11-03/a.parquet is new, but its day 2026-11-03 is below this load's range, which starts at 2026-11-04, and the table holds other files of day 2026-11-03",
            ),
        },
        SelectFilesTestCase {
            description: "under delete_insert, a new file below the window in a partition the table holds nothing of is skipped",
            loaded: &[],
            held: &[("/data/day=2026-11-01/gone.parquet", "1-0")],
            removed: false,
            cursor_type: Some(ColumnType::Date),
            bounds: (Some("2026-11-04"), None),
            expected_keys: Ok(&["day=2026-11-04/a.parquet", "day=2026-11-05/a.parquet"]),
        },
        SelectFilesTestCase {
            description: "a text cursor compares as text",
            loaded: &[],
            held: &[],
            removed: false,
            cursor_type: Some(ColumnType::Text),
            bounds: (Some("2026-11-05"), None),
            expected_keys: Ok(&["day=2026-11-05/a.parquet"]),
        },
        SelectFilesTestCase {
            description: "an int cursor needs integer values",
            loaded: &[],
            held: &[],
            removed: false,
            cursor_type: Some(ColumnType::Int),
            bounds: (Some("3"), None),
            expected_keys: Err("is not an integer"),
        },
        SelectFilesTestCase {
            description: "a timestamp partition cannot be a cursor",
            loaded: &[],
            held: &[],
            removed: false,
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
                member: None,
            },
            partitions: vec![("day".to_owned(), (*day).to_owned())],
        })
        .collect();
    let paths = PathValues {
        partition_columns: vec!["day".to_owned()],
        ..PathValues::default()
    };
    for test_case in test_cases {
        let loaded: Vec<LoadedFile> = test_case
            .loaded
            .iter()
            .map(|(name, version)| LoadedFile {
                name: (*name).to_owned(),
                version: (*version).to_owned(),
            })
            .collect();
        let held: Vec<LoadedFile> = test_case
            .held
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
            held: &held,
            removed: test_case.removed,
            cursor: test_case
                .cursor_type
                .as_ref()
                .map(|column_type| ("day", column_type)),
            paths: &paths,
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
