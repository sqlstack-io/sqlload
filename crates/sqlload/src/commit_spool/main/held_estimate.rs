//! How much a table's commit holds locally under `send = "at_commit"`.

/// About the most bytes one commit holds: the table's estimated size when it commits once (`once`), else no more than the size a commit is cut at (`commit_bytes`); `None` when the source gives no size.
pub fn held_estimate(estimated_bytes: Option<u64>, once: bool, commit_bytes: u64) -> Option<u64> {
    let estimated = estimated_bytes?;
    Some(match once {
        true => estimated,
        false => estimated.min(commit_bytes),
    })
}
