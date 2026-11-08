use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::adapters::contract::models::WriteMode;
use crate::adapters::files::main::remove_staged::remove_staged;
use crate::adapters::files::main::staged_folder::staged_folder;
use crate::adapters::files::main::swap_folder::swap_folder;
use crate::adapters::files::tests::helpers::{block_on, local_files, watch_folder};
use crate::adapters::files::tests::test_types::SwapFolderTestCase;
use crate::file_store::main::open_folder::open_folder;
use crate::store_paths::models::{Location, StoreKind};

/// The table folder below the scratch directory.
const TABLE_PREFIX: &str = "app/orders/";
/// Swaps per case, while another thread lists the table folder.
const SWAPS: usize = 40;

fn write_all(
    root: &std::path::Path,
    prefix: &str,
    keys: &[&str],
    body: &str,
) -> Result<(), String> {
    keys.iter().try_for_each(|key| {
        let path = root.join(prefix).join(key);
        std::fs::create_dir_all(path.parent().ok_or("no parent")?)
            .and_then(|()| std::fs::write(&path, body))
            .map_err(|error| error.to_string())
    })
}

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
        let swapped: Result<(), String> = (0..SWAPS).try_for_each(|swap| {
            let staged = staged_folder(&live, &WriteMode::Replace, &format!("load{swap}"))
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
        block_on(async {
            remove_staged(&live).await;
            Ok(())
        })?;
        let old: Vec<String> = test_case
            .replaced
            .iter()
            .map(|key| (*key).to_owned())
            .collect();
        let new: Vec<String> = vec!["part-new-0.csv".to_owned()];
        let allowed: BTreeSet<Vec<String>> = [old, new.clone()].into_iter().collect();
        assert!(
            seen.is_subset(&allowed),
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
