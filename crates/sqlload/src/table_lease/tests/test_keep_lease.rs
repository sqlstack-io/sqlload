use chrono::Utc;

use crate::table_lease::main::keep_lease::keep_lease;
use crate::table_lease::main::lease_holder::lease_holder;
use crate::table_lease::tests::helpers::{block_on, minutes_before, scratch_folder, taken};
use crate::table_lease::tests::test_types::KeepLeaseTestCase;

#[test]
fn given_a_holder_when_it_refreshes_its_lease_then_it_extends_it_unless_another_load_took_the_table()
-> Result<(), String> {
    let test_cases = [
        KeepLeaseTestCase {
            description: "a holder still alone extends its lease 10 minutes from now",
            taken_minutes_ago: 3,
            superseded: false,
            expected_outcome: "kept: the table is being loaded by load A",
        },
        KeepLeaseTestCase {
            description: "a holder whose lease expired and was taken by B learns it lost the table and writes nothing",
            taken_minutes_ago: 11,
            superseded: true,
            expected_outcome: "lost the table's lease to load B, as it was not refreshed for 10 minutes; stopped before deleting any files",
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
            for load in ["B"].iter().take(usize::from(test_case.superseded)) {
                taken(&folder, load, now).await?;
            }
            let kept = keep_lease(&folder, &lease, now)
                .await
                .map_err(|error| error.to_string())?;
            let ten_minutes_on = minutes_before(now, -9);
            let holder = lease_holder(&folder, ten_minutes_on)
                .await
                .map_err(|error| error.to_string())?
                .unwrap_or_default();
            Ok(kept.map_or_else(|lost| lost, |()| format!("kept: {holder}")))
        });
        let _ = std::fs::remove_dir_all(&root);
        let outcome = outcome?;
        assert!(
            outcome.starts_with(test_case.expected_outcome),
            "{}: {outcome}",
            test_case.description
        );
    }
    Ok(())
}
