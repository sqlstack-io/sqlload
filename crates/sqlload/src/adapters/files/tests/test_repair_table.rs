use bytes::Bytes;

use crate::adapters::files::_helpers::layout::markers::{decided_folder, marker_text};
use crate::adapters::files::errors::FilesDestinationError;
use crate::adapters::files::main::commit_replaced::commit_replaced;
use crate::adapters::files::main::mark_pending::mark_pending;
use crate::adapters::files::main::read_table_state::read_table_state;
use crate::adapters::files::main::repair_table::repair_table;
use crate::adapters::files::models::{PartitionBy, PendingPhase};
use crate::adapters::files::tests::helpers::{
    block_on, orders_path, repair_committed, scratch_folder,
};
use crate::adapters::files::tests::test_types::{
    AbortedTestCase, CommittedRepairTestCase, CompactTestCase, LoadedRecordsTestCase,
    RepairTestCase, StalledCommitTestCase,
};
use crate::column_types::models::ColumnType;
use crate::file_store::main::create_file::create_file;
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
            repair_table(&folder, (&state.pending, &state.records))
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

#[test]
fn given_records_of_loaded_source_files_when_listing_the_table_then_only_held_folders_of_held_commits_count()
-> Result<(), String> {
    let test_cases = [
        LoadedRecordsTestCase {
            description: "a held commit's files count, as do those of a commit stopped while replacing (its own files stay); a commit repair drops and one whose files are gone are stale, and the stale ones are never read (one is not even JSON)",
            records: &[
                (
                    "kept.00001.json",
                    "{\"commits\":{\"kept.00001\":{\"day=2026-11-04/\":[[\"s3://src/a.parquet\",\"v1\"]]}}}",
                ),
                (
                    "replacing.00001.json",
                    "{\"commits\":{\"replacing.00001\":{\"day=2026-11-05/\":[[\"s3://src/c.parquet\",\"v1\"]]}}}",
                ),
                (
                    "crashed.00001.json",
                    "{\"commits\":{\"crashed.00001\":{\"day=2026-11-04/\":[[\"s3://src/b.parquet\",\"v1\"]]}}}",
                ),
                ("gone.00001.json", "not json"),
            ],
            expected_loaded: &["s3://src/a.parquet", "s3://src/c.parquet"],
            expected_old: false,
            expected_stale: &[
                "_sqlload/loaded/crashed.00001.json",
                "_sqlload/loaded/gone.00001.json",
            ],
        },
        LoadedRecordsTestCase {
            description: "a commit that wrote two days, one since replaced by another load, still holds the file of the day it keeps, not the other's",
            records: &[(
                "kept.00001.json",
                "{\"commits\":{\"kept.00001\":{\"day=2026-11-04/\":[[\"s3://src/a.parquet\",\"v1\"]],\"day=2026-11-06/\":[[\"s3://src/d.parquet\",\"v1\"]]}}}",
            )],
            expected_loaded: &["s3://src/a.parquet"],
            expected_old: false,
            expected_stale: &[],
        },
        LoadedRecordsTestCase {
            description: "a merged record counts only what its held commits' folders hold, and one holding nothing held is stale",
            records: &[
                (
                    "compacted.one.json",
                    "{\"commits\":{\"kept.00001\":{\"day=2026-11-04/\":[[\"s3://src/e.parquet\",\"v1\"]]},\"gone.00001\":{\"day=2026-11-04/\":[[\"s3://src/f.parquet\",\"v1\"]]}}}",
                ),
                (
                    "compacted.two.json",
                    "{\"commits\":{\"gone.00002\":{\"\":[[\"s3://src/g.parquet\",\"v1\"]]}}}",
                ),
            ],
            expected_loaded: &["s3://src/e.parquet"],
            expected_old: false,
            expected_stale: &["_sqlload/loaded/compacted.two.json"],
        },
        LoadedRecordsTestCase {
            description: "a held commit's record in an older SQLLoad's format (a list of files) is never read as empty: the records cannot be read",
            records: &[("kept.00001.json", "[[\"s3://src/a.parquet\",\"v1\"]]")],
            expected_loaded: &[],
            expected_old: true,
            expected_stale: &[],
        },
    ];
    for test_case in test_cases {
        let (folder, root) = scratch_folder()?;
        let path = orders_path(Some(("day=", PartitionBy::Date)));
        let outcome = block_on(async {
            let failed = |error: FilesDestinationError| error.to_string();
            mark_pending(&folder, "crashed.00001", PendingPhase::Writing, (&[], None))
                .await
                .map_err(failed)?;
            mark_pending(
                &folder,
                "replacing.00001",
                PendingPhase::Replacing,
                (&[], None),
            )
            .await
            .map_err(failed)?;
            for key in [
                "day=2026-11-04/part-kept.00001-0.parquet",
                "day=2026-11-04/part-crashed.00001-0.parquet",
                "day=2026-11-05/part-replacing.00001-0.parquet",
                "day=2026-11-06/part-later.00001-0.parquet",
            ] {
                write_file(&folder, key, Bytes::from_static(b"x"))
                    .await
                    .map_err(|error| error.to_string())?;
            }
            for (key, text) in test_case.records {
                write_file(
                    &folder,
                    &format!("_sqlload/loaded/{key}"),
                    Bytes::from_static(text.as_bytes()),
                )
                .await
                .map_err(|error| error.to_string())?;
            }
            let state = read_table_state(&folder, &path, Some(&ColumnType::Date))
                .await
                .map_err(failed)?;
            let loaded: std::collections::BTreeSet<String> =
                state.loaded.into_iter().map(|file| file.name).collect();
            let old = state.old_records.is_some_and(|problem| {
                problem.contains("older SQLLoad") && problem.contains("--reload")
            });
            Ok::<_, String>((
                loaded.into_iter().collect::<Vec<String>>(),
                state.stale_loaded,
                old,
            ))
        });
        let _ = std::fs::remove_dir_all(&root);
        let outcome = outcome?;
        assert_eq!(
            outcome,
            (
                test_case
                    .expected_loaded
                    .iter()
                    .map(|name| (*name).to_owned())
                    .collect(),
                test_case
                    .expected_stale
                    .iter()
                    .map(|key| (*key).to_owned())
                    .collect(),
                test_case.expected_old
            ),
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_a_load_found_writing_when_it_stores_a_file_after_repair_then_the_next_repair_deletes_it_until_the_marker_expires()
-> Result<(), String> {
    let test_cases = [
        AbortedTestCase {
            description: "a part stored after the repair (its load had only lost its lease) is deleted, and the tombstone stays",
            marker_age_days: 0,
            expected_left: &["day=2026-11-04/part-kept-0.parquet"],
            expected_marker: true,
        },
        AbortedTestCase {
            description: "past 7 days the last repair still deletes the part it finds, then the tombstone goes",
            marker_age_days: 8,
            expected_left: &["day=2026-11-04/part-kept-0.parquet"],
            expected_marker: false,
        },
    ];
    for test_case in test_cases {
        let (folder, root) = scratch_folder()?;
        let path = orders_path(Some(("day=", PartitionBy::Date)));
        let outcome = block_on(async {
            let failed = |error: FilesDestinationError| error.to_string();
            let wrote = |error: crate::file_store::errors::StoreError| error.to_string();
            mark_pending(&folder, "lapsed", PendingPhase::Writing, (&[], None))
                .await
                .map_err(failed)?;
            for key in [
                "day=2026-11-04/part-lapsed-0.parquet",
                "day=2026-11-04/part-kept-0.parquet",
            ] {
                write_file(&folder, key, Bytes::from_static(b"x"))
                    .await
                    .map_err(wrote)?;
            }
            let state = read_table_state(&folder, &path, None)
                .await
                .map_err(failed)?;
            repair_table(&folder, (&state.pending, &state.records))
                .await
                .map_err(failed)?;
            let marker = root.join("orders/_sqlload/decided/lapsed");
            let age = std::time::Duration::from_secs(test_case.marker_age_days * 24 * 60 * 60);
            std::fs::File::options()
                .write(true)
                .open(&marker)
                .and_then(|file| file.set_modified(std::time::SystemTime::now() - age))
                .map_err(|error| error.to_string())?;
            write_file(
                &folder,
                "day=2026-11-04/part-lapsed-1.parquet",
                Bytes::from_static(b"x"),
            )
            .await
            .map_err(wrote)?;
            let state = read_table_state(&folder, &path, None)
                .await
                .map_err(failed)?;
            repair_table(&folder, (&state.pending, &state.records))
                .await
                .map_err(failed)?;
            let files = list_files(&folder).await.map_err(wrote)?;
            let left: Vec<String> = files.into_iter().map(|file| file.relative_key).collect();
            Ok::<_, String>((left, marker.exists()))
        });
        let _ = std::fs::remove_dir_all(&root);
        let (left, marker) = outcome?;
        assert_eq!(
            (left, marker),
            (
                test_case
                    .expected_left
                    .iter()
                    .map(|key| (*key).to_owned())
                    .collect::<Vec<String>>(),
                test_case.expected_marker
            ),
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_more_records_than_the_limit_when_repairing_then_one_merged_record_holds_what_they_held()
-> Result<(), String> {
    let test_cases = [
        CompactTestCase {
            description: "65 records merge into one that still holds every loaded file",
            records: 65,
            expected_merged: &[true],
        },
        CompactTestCase {
            description: "64 records stay as they are",
            records: 64,
            expected_merged: &[false; 64],
        },
    ];
    for test_case in test_cases {
        let (folder, root) = scratch_folder()?;
        let path = orders_path(None);
        let outcome = block_on(async {
            let failed = |error: FilesDestinationError| error.to_string();
            let wrote = |error: crate::file_store::errors::StoreError| error.to_string();
            for index in 0..test_case.records {
                let commit = format!("load{index}.00001");
                let part = format!("part-{commit}-0.parquet");
                write_file(&folder, &part, Bytes::from_static(b"x"))
                    .await
                    .map_err(wrote)?;
                let record = format!(
                    "{{\"commits\":{{\"{commit}\":{{\"\":[[\"s3://src/{index}.parquet\",\"v1\"]]}}}}}}"
                );
                let key = format!("_sqlload/loaded/{commit}.json");
                write_file(&folder, &key, Bytes::from(record))
                    .await
                    .map_err(wrote)?;
            }
            let before = read_table_state(&folder, &path, None)
                .await
                .map_err(failed)?;
            repair_table(&folder, (&before.pending, &before.records))
                .await
                .map_err(failed)?;
            let after = read_table_state(&folder, &path, None)
                .await
                .map_err(failed)?;
            let merged: Vec<bool> = after
                .records
                .keys
                .iter()
                .map(|key| key.starts_with("_sqlload/loaded/compacted."))
                .collect();
            Ok::<_, String>((after.loaded == before.loaded, before.loaded.len(), merged))
        });
        let _ = std::fs::remove_dir_all(&root);
        assert_eq!(
            outcome?,
            (true, test_case.records, test_case.expected_merged.to_vec()),
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_a_load_stalled_after_its_lease_check_when_another_load_repaired_the_table_then_its_commit_fails_without_deleting_anything()
-> Result<(), String> {
    let test_cases = [
        StalledCommitTestCase {
            description: "with no repair in between, the commit replaces the old file with its own",
            repairs: 0,
            repairs_listed_before_commit: 0,
            aborted_after_commit: 0,
            expected_failed: false,
            expected_left: &["day=2026-11-04/part-stalled.00001-0.parquet"],
            expected_decided: &[],
        },
        StalledCommitTestCase {
            description: "after another load repaired the table (deleting the stalled load's file), the resumed commit fails and the old file stays",
            repairs: 1,
            repairs_listed_before_commit: 0,
            aborted_after_commit: 0,
            expected_failed: true,
            expected_left: &["day=2026-11-04/part-old-0.parquet"],
            expected_decided: &["stalled.00001"],
        },
        StalledCommitTestCase {
            description: "a commit finishing after the repairing load listed the table keeps its file, and the repair gives nothing up",
            repairs: 0,
            repairs_listed_before_commit: 1,
            aborted_after_commit: 0,
            expected_failed: false,
            expected_left: &["day=2026-11-04/part-stalled.00001-0.parquet"],
            expected_decided: &[],
        },
        StalledCommitTestCase {
            description: "an aborted decision left after a finished commit, by a repair that stopped while withdrawing it, goes without the commit's file",
            repairs: 0,
            repairs_listed_before_commit: 0,
            aborted_after_commit: 1,
            expected_failed: false,
            expected_left: &["day=2026-11-04/part-stalled.00001-0.parquet"],
            expected_decided: &[],
        },
    ];
    for test_case in test_cases {
        let (folder, root) = scratch_folder()?;
        let path = orders_path(Some(("day=", PartitionBy::Date)));
        let outcome = block_on(async {
            let failed = |error: FilesDestinationError| error.to_string();
            let wrote = |error: crate::file_store::errors::StoreError| error.to_string();
            mark_pending(&folder, "stalled.00001", PendingPhase::Writing, (&[], None))
                .await
                .map_err(failed)?;
            for key in [
                "day=2026-11-04/part-old-0.parquet",
                "day=2026-11-04/part-stalled.00001-0.parquet",
            ] {
                write_file(&folder, key, Bytes::from_static(b"x"))
                    .await
                    .map_err(wrote)?;
            }
            let replaced = ["day=2026-11-04/part-old-0.parquet".to_owned()];
            for _ in 0..test_case.repairs {
                let state = read_table_state(&folder, &path, None)
                    .await
                    .map_err(failed)?;
                repair_table(&folder, (&state.pending, &state.records))
                    .await
                    .map_err(failed)?;
            }
            let mut listed = Vec::new();
            for _ in 0..test_case.repairs_listed_before_commit {
                listed.push(
                    read_table_state(&folder, &path, None)
                        .await
                        .map_err(failed)?,
                );
            }
            let resumed = commit_replaced(&folder, "stalled.00001", (&replaced, None)).await;
            for state in &listed {
                repair_table(&folder, (&state.pending, &state.records))
                    .await
                    .map_err(failed)?;
            }
            let aborted = marker_text(PendingPhase::Aborted, &[], None);
            for _ in 0..test_case.aborted_after_commit {
                create_file(
                    &decided_folder(&folder),
                    "stalled.00001",
                    Bytes::from(aborted.clone()),
                )
                .await
                .map_err(wrote)?;
            }
            let state = read_table_state(&folder, &path, None)
                .await
                .map_err(failed)?;
            repair_table(&folder, (&state.pending, &state.records))
                .await
                .map_err(failed)?;
            let files = list_files(&folder).await.map_err(wrote)?;
            let left: Vec<String> = files.into_iter().map(|file| file.relative_key).collect();
            let decided = list_files(&decided_folder(&folder)).await.map_err(wrote)?;
            let decided: Vec<String> = decided.into_iter().map(|file| file.relative_key).collect();
            Ok::<_, String>((resumed.is_err(), left, decided))
        });
        let _ = std::fs::remove_dir_all(&root);
        assert_eq!(
            outcome?,
            (
                test_case.expected_failed,
                test_case
                    .expected_left
                    .iter()
                    .map(|key| (*key).to_owned())
                    .collect::<Vec<String>>(),
                test_case
                    .expected_decided
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
