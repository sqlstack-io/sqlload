use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::adapters::contract::models::WriteMode;
use crate::adapters::files::_helpers::layout::swap::{carry_over, remove_staged, swap_in};
use crate::adapters::files::main::read_table_state::read_table_state;
use crate::adapters::files::main::repair_table::repair_table;
use crate::adapters::files::main::swap_folder::swap_folder;
use crate::adapters::files::models::TablePath;
use crate::adapters::files::tests::helpers::{
    block_on, local_files, top_parts, watch_folder, write_all,
};
use crate::adapters::files::tests::test_types::{
    LateSwapTestCase, RemoveStagedTestCase, RenameRaceTestCase, StalledSwapTestCase,
    SwapFolderTestCase,
};
use crate::file_store::main::open_folder::open_folder;
use crate::store_paths::models::{Location, StoreKind};
use crate::table_lease::main::lease_now::lease_now;
use crate::table_lease::main::take_lease::take_lease;

/// The table folder below the scratch directory.
const TABLE_PREFIX: &str = "app/orders/";
/// Swaps per case, while another thread lists the table folder.
const SWAPS: usize = 40;

#[test]
fn given_a_staged_table_when_swapping_it_in_then_readers_see_the_old_or_new_files_and_the_rest_is_kept()
-> Result<(), String> {
    let test_cases = [
        SwapFolderTestCase {
            description: "the run log, the lease, a tombstone's two markers and a file beside the data are carried over; replaced files are not",
            live: &[
                "part-old-0.csv",
                "part-old-1.csv",
                "_sqlload/runs/r1.json",
                "_sqlload/pending/crashed",
                "_sqlload/decided/crashed",
                "README.txt",
            ],
            replaced: &["part-old-0.csv", "part-old-1.csv"],
            staged: &["part-new-0.csv", "_sqlload/runs/r2.json"],
            expected_live: &[
                "README.txt",
                "_sqlload/decided/crashed",
                "_sqlload/lease/00000000000000000001",
                "_sqlload/pending/crashed",
                "_sqlload/runs/r1.json",
                "_sqlload/runs/r2.json",
                "part-new-0.csv",
            ],
        },
        SwapFolderTestCase {
            description: "a first load, whose folder holds only its lease, swaps its table in",
            live: &[],
            replaced: &[],
            staged: &["part-new-0.csv"],
            expected_live: &["_sqlload/lease/00000000000000000001", "part-new-0.csv"],
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
        let lease = block_on(async {
            let folder = open_folder(&live).map_err(|error| error.to_string())?;
            take_lease(&folder, "swapping", lease_now())
                .await
                .map_err(|error| error.to_string())?
        })?;
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
                swap_folder((&live_folder, &lease), &staged_folder, &replaced)
                    .await
                    .map_err(|error| error.to_string())
            })
        });
        stop.store(true, Ordering::Relaxed);
        let seen = watcher.join().map_err(|_| "the watcher panicked")?;
        swapped?;
        block_on(async {
            remove_staged(&live)
                .await
                .map_err(|error| error.to_string())
        })?;
        let old: Vec<String> = test_case
            .replaced
            .iter()
            .copied()
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
fn given_folders_left_beside_a_table_when_a_load_takes_it_then_staged_ones_go() -> Result<(), String>
{
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
            description: "an old table an earlier release moved aside is kept, the new one beside it goes",
            before: &[
                ".orders.sqlload-0b5e7a52-2f8a-4a0e-9a35-6f3f1d2c8e11-old/part-a-0.csv",
                ".orders.sqlload-0b5e7a52-2f8a-4a0e-9a35-6f3f1d2c8e11/part-b-0.csv",
                "orders/_sqlload/lease/00000000000000000001",
            ],
            expected_after: &[
                ".orders.sqlload-0b5e7a52-2f8a-4a0e-9a35-6f3f1d2c8e11-old/part-a-0.csv",
                "orders/_sqlload/lease/00000000000000000001",
            ],
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
        block_on(async {
            remove_staged(&live)
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

#[test]
fn given_a_load_stalled_before_its_swap_when_another_load_took_the_table_then_its_swap_fails_and_the_table_is_kept()
-> Result<(), String> {
    let test_cases = [
        StalledSwapTestCase {
            description: "with its lease held, the staged table is swapped in",
            lease_age_minutes: 0,
            takeovers: 0,
            expected_failed: false,
            expected_parts: &["part-new-0.csv"],
        },
        StalledSwapTestCase {
            description: "after its lease lapsed and another load took the table, the swap fails and the old table stays",
            lease_age_minutes: 11,
            takeovers: 1,
            expected_failed: true,
            expected_parts: &["part-old-0.csv"],
        },
    ];
    for test_case in test_cases {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let root = directory.path().display().to_string();
        let live = Location {
            store: StoreKind::Filesystem { root },
            prefix: TABLE_PREFIX.to_owned(),
        };
        write_all(directory.path(), TABLE_PREFIX, &["part-old-0.csv"], "old")?;
        let path = TablePath {
            folder: live.clone(),
            partition: None,
        };
        let load_id = uuid::Uuid::new_v4().to_string();
        let staged = path
            .staged_folder(&WriteMode::Replace, &load_id)
            .ok_or("no staging folder")?;
        write_all(directory.path(), &staged.prefix, &["part-new-0.csv"], "new")?;
        let replaced: BTreeSet<String> = BTreeSet::from(["part-old-0.csv".to_owned()]);
        let swapped = block_on(async {
            let failed = |error: String| error;
            let live_folder = open_folder(&live).map_err(|error| error.to_string())?;
            let staged_folder = open_folder(&staged).map_err(|error| error.to_string())?;
            let taken_at = lease_now()
                - chrono::Duration::minutes(
                    i64::try_from(test_case.lease_age_minutes)
                        .map_err(|error| error.to_string())?,
                );
            let lease = take_lease(&live_folder, &load_id, taken_at)
                .await
                .map_err(|error| error.to_string())?
                .map_err(failed)?;
            for _ in 0..test_case.takeovers {
                take_lease(&live_folder, "other", lease_now())
                    .await
                    .map_err(|error| error.to_string())?
                    .map_err(failed)?;
                remove_staged(&live)
                    .await
                    .map_err(|error| error.to_string())?;
            }
            Ok::<_, String>(
                swap_folder((&live_folder, &lease), &staged_folder, &replaced)
                    .await
                    .is_err(),
            )
        })?;
        assert_eq!(
            (swapped, top_parts(&directory.path().join(TABLE_PREFIX))),
            (
                test_case.expected_failed,
                test_case
                    .expected_parts
                    .iter()
                    .map(|key| (*key).to_owned())
                    .collect::<Vec<String>>()
            ),
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_a_load_that_checked_its_lease_when_another_load_takes_the_table_before_its_rename_then_the_rename_fails_and_the_table_is_kept()
-> Result<(), String> {
    let test_cases = [
        RenameRaceTestCase {
            description: "the rename comes first: the new table is in place, and the load taking the table deletes the old one beside it",
            takeovers: 0,
            expected_failed: false,
            expected_parts: &["part-new-0.csv"],
        },
        RenameRaceTestCase {
            description: "the other load moves the staged table away first: the rename fails and the old table stays",
            takeovers: 1,
            expected_failed: true,
            expected_parts: &["part-old-0.csv"],
        },
    ];
    for test_case in test_cases {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let root = directory.path().display().to_string();
        let live = Location {
            store: StoreKind::Filesystem { root },
            prefix: TABLE_PREFIX.to_owned(),
        };
        write_all(directory.path(), TABLE_PREFIX, &["part-old-0.csv"], "old")?;
        let staged = TablePath {
            folder: live.clone(),
            partition: None,
        }
        .staged_folder(&WriteMode::Replace, &uuid::Uuid::new_v4().to_string())
        .ok_or("no staging folder")?;
        write_all(directory.path(), &staged.prefix, &["part-new-0.csv"], "new")?;
        let staged_path = directory.path().join(&staged.prefix);
        let live_path = directory.path().join(TABLE_PREFIX);
        for _ in 0..test_case.takeovers {
            block_on(async {
                remove_staged(&live)
                    .await
                    .map_err(|error| error.to_string())
            })?;
        }
        let renamed = swap_in(&staged_path, &live_path);
        block_on(async {
            remove_staged(&live)
                .await
                .map_err(|error| error.to_string())
        })?;
        assert_eq!(
            (
                renamed.is_err(),
                top_parts(&live_path),
                local_files(&directory.path().join("app")).len()
            ),
            (
                test_case.expected_failed,
                test_case
                    .expected_parts
                    .iter()
                    .map(|key| (*key).to_owned())
                    .collect::<Vec<String>>(),
                1
            ),
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_a_stalled_swap_landing_after_another_load_took_the_table_when_that_load_repairs_then_it_stops_having_lost_the_lease()
-> Result<(), String> {
    let test_cases = [
        LateSwapTestCase {
            description: "with no late swap, the other load repairs and keeps the lease",
            swaps_after_take: false,
            expected_lost: false,
            expected_parts: &["part-old-0.csv"],
        },
        LateSwapTestCase {
            description: "a swap landing after the other load took the lease carries its lease away: its repair stops, and the swapped-in table is whole",
            swaps_after_take: true,
            expected_lost: true,
            expected_parts: &["part-new-0.csv"],
        },
    ];
    for test_case in test_cases {
        let directory = tempfile::tempdir().map_err(|error| error.to_string())?;
        let root = directory.path().display().to_string();
        let live = Location {
            store: StoreKind::Filesystem { root },
            prefix: TABLE_PREFIX.to_owned(),
        };
        write_all(directory.path(), TABLE_PREFIX, &["part-old-0.csv"], "old")?;
        let load_id = uuid::Uuid::new_v4().to_string();
        let staged = TablePath {
            folder: live.clone(),
            partition: None,
        }
        .staged_folder(&WriteMode::Replace, &load_id)
        .ok_or("no staging folder")?;
        let (staged_path, live_path) = (
            directory.path().join(&staged.prefix),
            directory.path().join(TABLE_PREFIX),
        );
        let lost = block_on(async {
            let failed = |error: String| error;
            let folder = open_folder(&live).map_err(|error| error.to_string())?;
            let taken_at = lease_now() - chrono::Duration::minutes(11);
            take_lease(&folder, &load_id, taken_at)
                .await
                .map_err(|error| error.to_string())?
                .map_err(failed)?;
            write_all(directory.path(), &staged.prefix, &["part-new-0.csv"], "new")?;
            let staged_folder = open_folder(&staged).map_err(|error| error.to_string())?;
            carry_over(
                &folder,
                &staged_folder,
                &BTreeSet::from(["part-old-0.csv".to_owned()]),
            )
            .await
            .map_err(|(key, reason)| format!("{key}: {reason}"))?;
            let lease = take_lease(&folder, "taker", lease_now())
                .await
                .map_err(|error| error.to_string())?
                .map_err(failed)?;
            for _ in 0..usize::from(test_case.swaps_after_take) {
                swap_in(&staged_path, &live_path).map_err(|error| error.to_string())?;
            }
            let state = read_table_state(
                &folder,
                &TablePath {
                    folder: live.clone(),
                    partition: None,
                },
                None,
            )
            .await
            .map_err(|error| error.to_string())?;
            let repaired = repair_table((&folder, &lease), (&state.pending, &state.records))
                .await
                .map_err(|error| error.to_string())?;
            Ok::<_, String>(repaired.is_err())
        })?;
        assert_eq!(
            (lost, top_parts(&live_path)),
            (
                test_case.expected_lost,
                test_case
                    .expected_parts
                    .iter()
                    .map(|key| (*key).to_owned())
                    .collect::<Vec<String>>()
            ),
            "{}",
            test_case.description
        );
    }
    Ok(())
}
