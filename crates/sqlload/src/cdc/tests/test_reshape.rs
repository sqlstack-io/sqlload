use crate::cdc::_helpers::reshape::reshape_problem;
use crate::cdc::_helpers::snapshot_log::{newest_snapshot, read_record};
use crate::cdc::models::{PlannedShape, SnapshotRecord};
use crate::cdc::tests::helpers::{catalog_column, record};
use crate::cdc::tests::test_types::{ReshapeTestCase, SnapshotRecordsTestCase};

const LIVE: (bool, bool) = (false, false);
const DROPPED: (bool, bool) = (true, false);
const DEFAULTED: (bool, bool) = (false, true);

/// A slot resumes across the column changes its stream carries, recording the table's new shape, and refuses the ones whose values no change carries.
#[test]
fn given_a_table_changed_since_its_snapshot_when_the_slot_resumes_then_only_changes_the_stream_carries_are_accepted()
 {
    let recorded = || record(",id,a,", Some(3), Some(",id,"));
    let base = || {
        vec![
            catalog_column("id", 1, LIVE),
            catalog_column("a", 2, LIVE),
            catalog_column("........pg.dropped.3........", 3, DROPPED),
        ]
    };
    let with = |extra: Vec<crate::cdc::models::CatalogColumn>| [base(), extra].concat();
    let test_cases = [
        ReshapeTestCase {
            description: "unchanged columns keep the record",
            recorded: recorded(),
            planned: (",id,a,", ",id,", true),
            known: vec!["id", "a"],
            catalog: base(),
            expected_rewrite: Ok(None),
        },
        ReshapeTestCase {
            description: "a column added after the record without a default is recorded",
            recorded: recorded(),
            planned: (",id,a,b,", ",id,", true),
            known: vec!["id", "a", "b"],
            catalog: with(vec![catalog_column("b", 4, LIVE)]),
            expected_rewrite: Ok(Some(record(",id,a,b,", Some(4), Some(",id,")))),
        },
        ReshapeTestCase {
            description: "a column added with a default is refused",
            recorded: recorded(),
            planned: (",id,a,b,", ",id,", true),
            known: vec!["id", "a", "b"],
            catalog: with(vec![catalog_column("b", 4, DEFAULTED)]),
            expected_rewrite: Err(
                "column b of t was added with a default (or as an identity or generated column), which fills existing rows without changes to stream; run `sqlload load --resnapshot` to copy every table again, or exclude the column",
            ),
        },
        ReshapeTestCase {
            description: "a dropped column is recorded gone, keeping the highest number",
            recorded: recorded(),
            planned: (",id,", ",id,", true),
            known: vec!["id"],
            catalog: vec![
                catalog_column("id", 1, LIVE),
                catalog_column("........pg.dropped.2........", 2, DROPPED),
                catalog_column("........pg.dropped.3........", 3, DROPPED),
            ],
            expected_rewrite: Ok(Some(SnapshotRecord {
                left: ",a,".to_owned(),
                ..record(",id,", Some(3), Some(",id,"))
            })),
        },
        ReshapeTestCase {
            description: "a column dropped and added back between loads has a number above the highest, so it is refused",
            recorded: recorded(),
            planned: (",id,a,", ",id,", true),
            known: vec!["id", "a"],
            catalog: vec![
                catalog_column("id", 1, LIVE),
                catalog_column("........pg.dropped.2........", 2, DROPPED),
                catalog_column("........pg.dropped.3........", 3, DROPPED),
                catalog_column("a", 4, LIVE),
            ],
            expected_rewrite: Err(
                "t loads column a again after it stopped loading (dropped and added back, or excluded and included), so its destination values are stale; run `sqlload load --resnapshot` to copy every table again",
            ),
        },
        ReshapeTestCase {
            description: "a column recorded as no longer loaded and added back is refused",
            recorded: SnapshotRecord {
                left: ",a,".to_owned(),
                ..record(",id,", Some(3), Some(",id,"))
            },
            planned: (",id,a,", ",id,", true),
            known: vec!["id", "a"],
            catalog: vec![
                catalog_column("id", 1, LIVE),
                catalog_column("........pg.dropped.2........", 2, DROPPED),
                catalog_column("........pg.dropped.3........", 3, DROPPED),
                catalog_column("a", 4, LIVE),
            ],
            expected_rewrite: Err(
                "t loads column a again after it stopped loading (dropped and added back, or excluded and included), so its destination values are stale; run `sqlload load --resnapshot` to copy every table again",
            ),
        },
        ReshapeTestCase {
            description: "a column excluded since the snapshot and included again is refused",
            recorded: SnapshotRecord {
                left: ",a,".to_owned(),
                ..record(",id,", Some(3), Some(",id,"))
            },
            planned: (",id,a,", ",id,", true),
            known: vec!["id", "a"],
            catalog: base(),
            expected_rewrite: Err(
                "t loads column a again after it stopped loading (dropped and added back, or excluded and included), so its destination values are stale; run `sqlload load --resnapshot` to copy every table again",
            ),
        },
        ReshapeTestCase {
            description: "a column planned and gone again by the catalog read asks for a rerun",
            recorded: recorded(),
            planned: (",id,a,b,", ",id,", true),
            known: vec!["id", "a", "b"],
            catalog: base(),
            expected_rewrite: Err(
                "t loads column b, which the source table no longer has: it changed shape during this load, so run it again",
            ),
        },
        ReshapeTestCase {
            description: "a renamed column keeps its number, so it is refused",
            recorded: recorded(),
            planned: (",id,c,", ",id,", true),
            known: vec!["id", "c"],
            catalog: vec![catalog_column("id", 1, LIVE), catalog_column("c", 2, LIVE)],
            expected_rewrite: Err(
                "t now loads column c, which the table already had at its snapshot (renamed, or newly included), so its rows hold values no change carries; run `sqlload load --resnapshot` to copy every table again",
            ),
        },
        ReshapeTestCase {
            description: "a column excluded when the record was written and loaded now is refused",
            recorded: record(",id,a,", Some(5), Some(",id,")),
            planned: (",id,a,z,", ",id,", true),
            known: vec!["id", "a", "z"],
            catalog: with(vec![catalog_column("z", 5, LIVE)]),
            expected_rewrite: Err(
                "t now loads column z, which the table already had at its snapshot (renamed, or newly included), so its rows hold values no change carries; run `sqlload load --resnapshot` to copy every table again",
            ),
        },
        ReshapeTestCase {
            description: "a column added but excluded raises the recorded highest number, so loading it later is refused",
            recorded: recorded(),
            planned: (",id,a,", ",id,", true),
            known: vec!["id", "a", "z"],
            catalog: with(vec![catalog_column("z", 5, LIVE)]),
            expected_rewrite: Ok(Some(record(",id,a,", Some(5), Some(",id,")))),
        },
        ReshapeTestCase {
            description: "a column added after the load was planned does not raise the highest number",
            recorded: recorded(),
            planned: (",id,a,", ",id,", true),
            known: vec!["id", "a"],
            catalog: with(vec![catalog_column("w", 6, LIVE)]),
            expected_rewrite: Ok(None),
        },
        ReshapeTestCase {
            description: "a key change under merge is refused",
            recorded: recorded(),
            planned: (",id,a,", ",a,", true),
            known: vec!["id", "a"],
            catalog: base(),
            expected_rewrite: Err(
                "the key of t changed since its snapshot, so rows deleted before could share a new key; run `sqlload load --resnapshot` to copy every table again",
            ),
        },
        ReshapeTestCase {
            description: "a key change in a changelog is recorded",
            recorded: recorded(),
            planned: (",id,a,", ",a,", false),
            known: vec!["id", "a"],
            catalog: base(),
            expected_rewrite: Ok(Some(record(",id,a,", Some(3), Some(",a,")))),
        },
        ReshapeTestCase {
            description: "a record without column numbers and unchanged columns is upgraded",
            recorded: record(",id,a,", None, None),
            planned: (",id,a,", ",id,", true),
            known: vec!["id", "a"],
            catalog: base(),
            expected_rewrite: Ok(Some(record(",id,a,", Some(2), Some(",id,")))),
        },
        ReshapeTestCase {
            description: "a record without column numbers refuses any column change",
            recorded: record(",id,a,", None, None),
            planned: (",id,a,b,", ",id,", true),
            known: vec!["id", "a", "b"],
            catalog: with(vec![catalog_column("b", 4, LIVE)]),
            expected_rewrite: Err(
                "the columns of t changed since its snapshot, which predates tracking schema changes; run `sqlload load --resnapshot` to copy every table again",
            ),
        },
    ];
    for test_case in test_cases {
        let known: Vec<String> = test_case
            .known
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        let (columns, key, merge) = test_case.planned;
        let planned = PlannedShape {
            columns,
            key,
            merge,
            known: &known,
        };
        assert_eq!(
            reshape_problem(&test_case.recorded, planned, &test_case.catalog),
            test_case.expected_rewrite.map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
}

/// Records written before column numbers were kept still read, and of a table's records under the newest snapshot the newest wins.
#[test]
fn given_snapshot_records_when_reading_them_then_old_and_new_forms_parse_and_the_newest_per_table_wins()
 {
    let test_cases = [
        SnapshotRecordsTestCase {
            description: "the newest snapshot's tables, each by its newest record, in either form",
            rows: vec![
                "5\t2026-01-01T00:00:00.000000Z\t\t",
                "5\t2026-01-01T00:00:01.000000Z\tt\t,id,a,",
                "5\t2026-01-02T00:00:00.000000Z\tt\t,id,a,b,\t4\t,id,\t,",
                "5\t2026-01-01T00:00:02.000000Z\tu\t,k,",
                "3\t2025-12-01T00:00:00.000000Z\t\t",
                "3\t2025-12-01T00:00:01.000000Z\tv\t,x,",
            ],
            expected_snapshot: Ok(Some((
                5,
                vec![
                    record(",id,a,b,", Some(4), Some(",id,")),
                    SnapshotRecord {
                        table: "u".to_owned(),
                        ..record(",k,", None, None)
                    },
                ],
            ))),
        },
        SnapshotRecordsTestCase {
            description: "a record with a column number but no key is unreadable",
            rows: vec!["5\tts\tt\t,a,\t4"],
            expected_snapshot: Err("an unreadable snapshot record: \"5\\tts\\tt\\t,a,\\t4\""),
        },
    ];
    for test_case in test_cases {
        let records: Result<Vec<_>, String> =
            test_case.rows.iter().map(|row| read_record(row)).collect();
        assert_eq!(
            records.map(newest_snapshot),
            test_case.expected_snapshot.map_err(str::to_owned),
            "{}",
            test_case.description
        );
    }
}
