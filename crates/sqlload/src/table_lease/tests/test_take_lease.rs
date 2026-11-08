use chrono::Utc;

use crate::table_lease::main::lease_holder::lease_holder;
use crate::table_lease::main::release_lease::release_lease;
use crate::table_lease::main::take_lease::take_lease;
use crate::table_lease::tests::helpers::{
    block_on, lease_files, minutes_before, scratch_folder, taken,
};
use crate::table_lease::tests::test_types::{RaceTestCase, TakeLeaseTestCase};

#[test]
fn given_earlier_loads_when_a_load_takes_a_files_table_then_it_waits_for_a_live_lease_and_takes_over_an_expired_one()
-> Result<(), String> {
    let test_cases = [
        TakeLeaseTestCase {
            description: "a table nobody has loaded is taken at generation 1",
            earlier: &[],
            released: 0,
            expected_outcome: "taken 1; files 00000000000000000001",
        },
        TakeLeaseTestCase {
            description: "a lease taken a minute ago is live: B is refused and told who holds it",
            earlier: &[("A", 1)],
            released: 0,
            expected_outcome: "the table is being loaded by load A (started ",
        },
        TakeLeaseTestCase {
            description: "a lease 11 minutes old has expired: B takes the next generation and the old one is deleted",
            earlier: &[("A", 11)],
            released: 0,
            expected_outcome: "taken 2; files 00000000000000000002",
        },
        TakeLeaseTestCase {
            description: "a released lease frees the table at once, and the next load takes the next generation, never one used before",
            earlier: &[("A", 1)],
            released: 1,
            expected_outcome: "taken 2; files 00000000000000000002",
        },
        TakeLeaseTestCase {
            description: "a load whose clock runs a minute behind the releasing load's still finds the released table free",
            earlier: &[("A", -1)],
            released: 1,
            expected_outcome: "taken 2; files 00000000000000000002",
        },
        TakeLeaseTestCase {
            description: "only the newest generation counts: A's expired lease taken over by a live C still refuses B",
            earlier: &[("A", 30), ("C", 2)],
            released: 0,
            expected_outcome: "the table is being loaded by load C (started ",
        },
    ];
    for test_case in test_cases {
        let (folder, root) = scratch_folder()?;
        let now = Utc::now();
        let outcome = block_on(async {
            let mut leases = Vec::new();
            for (load, minutes) in test_case.earlier {
                leases.push(taken(&folder, load, minutes_before(now, *minutes)).await?);
            }
            for lease in leases.iter().take(test_case.released) {
                release_lease(&folder, lease)
                    .await
                    .map_err(|error| error.to_string())?;
            }
            let attempt = take_lease(&folder, "B", now)
                .await
                .map_err(|error| error.to_string())?;
            let files = lease_files(&folder).await?.join(",");
            Ok(attempt.map_or_else(
                |refusal| refusal,
                |lease| format!("taken {}; files {files}", lease.generation),
            ))
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

#[test]
fn given_two_loads_starting_together_when_both_take_a_free_table_then_exactly_one_holds_it()
-> Result<(), String> {
    let test_cases = [RaceTestCase {
        description: "of two loads taking generation 1 at once, the create-only put lets exactly one win",
        rounds: 25,
        expected_winners: 1,
    }];
    for test_case in test_cases {
        for round in 0..test_case.rounds {
            let (folder, root) = scratch_folder()?;
            let now = Utc::now();
            let outcome = block_on(async {
                let (first, second) = futures_util::future::join(
                    take_lease(&folder, "A", now),
                    take_lease(&folder, "B", now),
                )
                .await;
                let attempts = [first, second]
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| error.to_string())?;
                let holder = lease_holder(&folder, now)
                    .await
                    .map_err(|error| error.to_string())?;
                Ok((
                    attempts.iter().filter(|attempt| attempt.is_ok()).count(),
                    holder.is_some(),
                ))
            });
            let _ = std::fs::remove_dir_all(&root);
            assert_eq!(
                outcome?,
                (test_case.expected_winners, true),
                "{}: round {round}",
                test_case.description
            );
        }
    }
    Ok(())
}
