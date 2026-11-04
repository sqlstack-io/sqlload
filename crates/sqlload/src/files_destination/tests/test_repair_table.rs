use bytes::Bytes;

use crate::column_types::models::ColumnType;
use crate::file_store::main::list_files::list_files;
use crate::file_store::main::write_file::write_file;
use crate::files_destination::errors::FilesDestinationError;
use crate::files_destination::main::mark_pending::mark_pending;
use crate::files_destination::main::read_table_state::read_table_state;
use crate::files_destination::main::repair_table::repair_table;
use crate::files_destination::models::{PartitionBy, PendingPhase};
use crate::files_destination::tests::helpers::{block_on, orders_path, scratch_folder};
use crate::files_destination::tests::test_types::RepairTestCase;

#[test]
fn given_a_failed_write_that_kept_its_marker_when_repairing_then_its_files_go_by_load_id()
-> Result<(), String> {
    let test_cases = [RepairTestCase {
        description: "a file a failed put committed is found by load id and deleted with the marker",
        keys: &[
            "day=2026-11-04/part-crashed-0.parquet",
            "day=2026-11-05/part-crashed-7.parquet",
            "day=2026-11-04/part-kept-0.parquet",
        ],
        expected_left: &["day=2026-11-04/part-kept-0.parquet"],
    }];
    for test_case in test_cases {
        let (folder, root) = scratch_folder()?;
        let path = orders_path(Some(("day=", PartitionBy::Date)));
        let left: Vec<String> = block_on(async {
            let failed = |error: FilesDestinationError| error.to_string();
            mark_pending(&folder, "crashed", PendingPhase::Writing, &[])
                .await
                .map_err(failed)?;
            for key in test_case.keys {
                write_file(&folder, key, Bytes::from_static(b"x"))
                    .await
                    .map_err(|error| error.to_string())?;
            }
            let state = read_table_state(&folder, &path, Some(&ColumnType::Date))
                .await
                .map_err(failed)?;
            repair_table(&folder, &state.pending)
                .await
                .map_err(failed)?;
            let files = list_files(&folder)
                .await
                .map_err(|error| error.to_string())?;
            Ok(files.into_iter().map(|file| file.relative_key).collect())
        })?;
        let again = block_on(async {
            read_table_state(&folder, &path, None)
                .await
                .map(|state| state.pending.len())
                .map_err(|error| error.to_string())
        })?;
        let _ = std::fs::remove_dir_all(&root);
        assert_eq!(left, test_case.expected_left, "{}", test_case.description);
        assert_eq!(again, 0, "{}: the marker is gone", test_case.description);
    }
    Ok(())
}
