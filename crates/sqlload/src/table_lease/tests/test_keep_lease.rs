use chrono::Utc;

use crate::table_lease::main::keep_lease::keep_lease;
use crate::table_lease::main::lease_holder::lease_holder;
use crate::table_lease::main::release_lease::release_lease;
use crate::table_lease::tests::helpers::{block_on, minutes_before, scratch_folder, taken};
use crate::table_lease::tests::test_types::KeepLeaseTestCase;

#[test]
fn given_a_holder_when_it_refreshes_and_releases_its_lease_then_it_keeps_the_table_only_while_no_other_load_could_take_it()
-> Result<(), String> {
    let test_cases = [
        KeepLeaseTestCase {
            description: "a holder still alone extends its lease 10 minutes from now, and its release frees the table",
            taken_minutes_ago: 3,
            others: &[],
            expected_outcome: "kept: the table is being loaded by load A",
            expected_after_release: "free",
        },
        KeepLeaseTestCase {
            description: "a holder whose lease lapsed stops even though no other load took the table yet",
            taken_minutes_ago: 11,
            others: &[],
            expected_outcome: "the table's lease lapsed at ",
            expected_after_release: "free",
        },
        KeepLeaseTestCase {
            description: "a holder whose lease expired and was taken by B learns it lost the table, and its release leaves B's lease alone",
            taken_minutes_ago: 11,
            others: &[("B", false)],
            expected_outcome: "lost the table's lease to load B, as it was not refreshed for 10 minutes; stopped before deleting any files",
            expected_after_release: "the table is being loaded by load B",
        },
        KeepLeaseTestCase {
            description: "a holder whose table B took and already released still learns it lost the table",
            taken_minutes_ago: 11,
            others: &[("B", true)],
            expected_outcome: "lost the table's lease to load B",
            expected_after_release: "free",
        },
        KeepLeaseTestCase {
            description: "after B took and released the table, C holds the next generation: the stalled holder lost it, and its release leaves C's lease alone",
            taken_minutes_ago: 11,
            others: &[("B", true), ("C", false)],
            expected_outcome: "lost the table's lease to load C",
            expected_after_release: "the table is being loaded by load C",
        },
    ];
    for test_case in test_cases {
        let (folder, root) = scratch_folder()?;
        let now = Utc::now();
        let outcome = block_on(async {
            let lease = taken(
                &folder,
                "A",
                minutes_before(now, test_case.taken_minutes_ago),
            )
            .await?;
            for (load, releases) in test_case.others {
                let other = taken(&folder, load, now).await?;
                for released in [&other].into_iter().take(usize::from(*releases)) {
                    release_lease(&folder, released)
                        .await
                        .map_err(|error| error.to_string())?;
                }
            }
            let kept = keep_lease(&folder, &lease, now)
                .await
                .map_err(|error| error.to_string())?;
            let nine_minutes_on = minutes_before(now, -9);
            let holder = lease_holder(&folder, nine_minutes_on)
                .await
                .map_err(|error| error.to_string())?
                .unwrap_or_default();
            release_lease(&folder, &lease)
                .await
                .map_err(|error| error.to_string())?;
            let after = lease_holder(&folder, now)
                .await
                .map_err(|error| error.to_string())?
                .unwrap_or_else(|| "free".to_owned());
            Ok((
                kept.map_or_else(|lost| lost, |()| format!("kept: {holder}")),
                after,
            ))
        });
        let _ = std::fs::remove_dir_all(&root);
        let (outcome, after) = outcome?;
        assert!(
            outcome.starts_with(test_case.expected_outcome),
            "{}: {outcome}",
            test_case.description
        );
        assert!(
            after.starts_with(test_case.expected_after_release),
            "{}: after release, {after}",
            test_case.description
        );
    }
    Ok(())
}
