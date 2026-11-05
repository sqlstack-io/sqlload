use bytes::Bytes;

use crate::adapters::files::errors::FilesDestinationError;
use crate::adapters::files::main::mark_pending::mark_pending;
use crate::adapters::files::main::read_table_state::read_table_state;
use crate::adapters::files::main::repair_table::repair_table;
use crate::adapters::files::models::{PartitionBy, PendingPhase};
use crate::adapters::files::tests::helpers::{
    block_on, orders_path, repair_committed, scratch_folder,
};
use crate::adapters::files::tests::test_types::{CommittedRepairTestCase, RepairTestCase};
use crate::column_types::models::ColumnType;
use crate::file_store::main::list_files::list_files;
use crate::file_store::main::write_file::write_file;

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
            mark_pending(&folder, "crashed", PendingPhase::Writing, (&[], None))
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

#[test]
fn given_a_committed_load_with_a_run_record_when_repairing_then_the_record_is_written_and_the_replaced_files_go()
-> Result<(), String> {
    let test_cases = [CommittedRepairTestCase {
        description: "the marker's record is written before the replaced files go",
        keys: &["part-old-0.parquet", "part-load-0.parquet"],
        replaced: &["part-old-0.parquet"],
        expected_left: &["part-load-0.parquet"],
        expected_record: "{\"event\":\"run\"}",
    }];
    for test_case in test_cases {
        let (left, record) = repair_committed(&test_case)?;
        assert_eq!(left, test_case.expected_left, "{}", test_case.description);
        assert_eq!(
            record, test_case.expected_record,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
