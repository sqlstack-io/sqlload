use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::adapters::contract::models::WriteMode;
use crate::adapters::files::main::repair_table::repair_table;
use crate::adapters::files::main::swap_folder::swap_folder;
use crate::adapters::files::models::TablePath;
use crate::adapters::files::tests::helpers::{
    NO_FOLDER, block_on, local_files, watch_folder, write_all,
};
use crate::adapters::files::tests::test_types::{RemoveStagedTestCase, SwapFolderTestCase};
use crate::file_store::main::open_folder::open_folder;
use crate::store_paths::models::{Location, StoreKind};

/// The table folder below the scratch directory.
const TABLE_PREFIX: &str = "app/orders/";
/// Swaps per case, while another thread lists the table folder.
const SWAPS: usize = 40;

#[test]
fn given_a_staged_table_when_swapping_it_in_then_readers_see_the_old_or_new_files_and_the_rest_is_kept()
-> Result<(), String> {
    let test_cases = [
        SwapFolderTestCase {
            description: "the run log and a file beside the data are carried over; pending markers and replaced files are not",
            live: &[
                "part-old-0.csv",
                "part-old-1.csv",
                "_sqlload/runs/r1.json",
                "_sqlload/pending/crashed",
                "README.txt",
            ],
            replaced: &["part-old-0.csv", "part-old-1.csv"],
            staged: &["part-new-0.csv", "_sqlload/runs/r2.json"],
            expected_live: &[
                "README.txt",
                "_sqlload/runs/r1.json",
                "_sqlload/runs/r2.json",
                "part-new-0.csv",
            ],
        },
        SwapFolderTestCase {
            description: "a first load renames its folder into place",
            live: &[],
            replaced: &[],
            staged: &["part-new-0.csv"],
            expected_live: &["part-new-0.csv"],
        },
    ];
    for test_case in test_cases {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let store = StoreKind::Filesystem {
            root: directory.path().display().to_string(),
        };
        let live = Location {
            store: store.clone(),
            prefix: TABLE_PREFIX.to_owned(),
        };
        let live_path = directory.path().join(TABLE_PREFIX);
        write_all(directory.path(), TABLE_PREFIX, test_case.live, "old")?;
        let replaced: BTreeSet<String> = test_case
            .replaced
            .iter()
            .map(|key| (*key).to_owned())
            .collect();
        let stop = Arc::new(AtomicBool::new(false));
        let watcher = watch_folder(live_path.clone(), stop.clone());
        let swapped: Result<(), String> = (0..SWAPS).try_for_each(|_| {
            let path = TablePath {
                folder: live.clone(),
                partition: None,
            };
            let staged = path
                .staged_folder(&WriteMode::Replace, &uuid::Uuid::new_v4().to_string())
                .ok_or("no staging folder")?;
            write_all(directory.path(), &staged.prefix, test_case.staged, "new")?;
            let (live_folder, staged_folder) = (
                open_folder(&live).map_err(|error| error.to_string())?,
                open_folder(&staged).map_err(|error| error.to_string())?,
            );
            block_on(async {
                swap_folder(&live_folder, &staged_folder, &replaced)
                    .await
                    .map_err(|error| error.to_string())
            })
        });
        stop.store(true, Ordering::Relaxed);
        let seen = watcher.join().map_err(|_| "the watcher panicked")?;
        swapped?;
        let folder = open_folder(&live).map_err(|error| error.to_string())?;
        block_on(async {
            repair_table(&folder, &[])
                .await
                .map_err(|error| error.to_string())
        })?;
        let old: Vec<String> = test_case
            .replaced
            .iter()
            .copied()
            .chain(test_case.live.is_empty().then_some(NO_FOLDER))
            .map(str::to_owned)
            .collect();
        let new: Vec<String> = vec!["part-new-0.csv".to_owned()];
        let mixed: Vec<&Vec<String>> = seen
            .iter()
            .filter(|names| *names != &new && !names.iter().all(|name| old.contains(name)))
            .collect();
        assert!(
            mixed.is_empty(),
            "{}: a reader saw {seen:?}",
            test_case.description
        );
        assert!(
            seen.contains(&new),
            "{}: the reader never saw the new files",
            test_case.description
        );
        assert_eq!(
            local_files(&live_path),
            test_case.expected_live,
            "{}",
            test_case.description
        );
        assert_eq!(
            local_files(&directory.path().join("app")).len(),
            test_case.expected_live.len(),
            "{}: nothing is left beside the table folder",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_folders_left_beside_a_table_when_repairing_then_staged_ones_go_and_a_moved_aside_table_comes_back()
-> Result<(), String> {
    let test_cases = [
        RemoveStagedTestCase {
            description: "a stopped load's staging folder is deleted, other folders that only look alike are kept",
            before: &[
                "orders/part-a-0.csv",
                ".orders.sqlload-0b5e7a52-2f8a-4a0e-9a35-6f3f1d2c8e11/part-b-0.csv",
                ".orders.sqlload-x/keep.csv",
                "orders.sqlload-0b5e7a52-2f8a-4a0e-9a35-6f3f1d2c8e11/keep.csv",
            ],
            expected_after: &[
                ".orders.sqlload-x/keep.csv",
                "orders.sqlload-0b5e7a52-2f8a-4a0e-9a35-6f3f1d2c8e11/keep.csv",
                "orders/part-a-0.csv",
            ],
        },
        RemoveStagedTestCase {
            description: "a table moved aside by a stopped swap is put back, and the new one beside it deleted",
            before: &[
                ".orders.sqlload-0b5e7a52-2f8a-4a0e-9a35-6f3f1d2c8e11-old/part-a-0.csv",
                ".orders.sqlload-0b5e7a52-2f8a-4a0e-9a35-6f3f1d2c8e11-old/_sqlload/runs/r1.json",
                ".orders.sqlload-0b5e7a52-2f8a-4a0e-9a35-6f3f1d2c8e11/part-b-0.csv",
            ],
            expected_after: &["orders/_sqlload/runs/r1.json", "orders/part-a-0.csv"],
        },
    ];
    for test_case in test_cases {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        write_all(directory.path(), "app/", test_case.before, "x")?;
        let live = Location {
            store: StoreKind::Filesystem {
                root: directory.path().display().to_string(),
            },
            prefix: TABLE_PREFIX.to_owned(),
        };
        let folder = open_folder(&live).map_err(|error| error.to_string())?;
        block_on(async {
            repair_table(&folder, &[])
                .await
                .map_err(|error| error.to_string())
        })?;
        assert_eq!(
            local_files(&directory.path().join("app")),
            test_case.expected_after,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
