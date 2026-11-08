/// Loads that took a table before load `B` tries to, and what `B` gets.
pub(crate) struct TakeLeaseTestCase {
    pub(crate) description: &'static str,
    /// Each earlier load and how many minutes ago it took the lease (a lease lasts 10).
    pub(crate) earlier: &'static [(&'static str, i64)],
    /// How many of the earlier leases (oldest first) were released.
    pub(crate) released: usize,
    /// `taken <generation>; files <lease files left>`, or the start of the refusal.
    pub(crate) expected_outcome: &'static str,
}

/// A holder refreshing its lease after another load may have taken the table.
pub(crate) struct KeepLeaseTestCase {
    pub(crate) description: &'static str,
    /// How many minutes ago `A` took the lease.
    pub(crate) taken_minutes_ago: i64,
    /// Whether `B` then takes the table now (only possible once `A`'s lease expired).
    pub(crate) superseded: bool,
    /// `kept until +<minutes from now>`, or the start of why `A` must stop.
    pub(crate) expected_outcome: &'static str,
}

/// Two loads taking one free table at the same moment, again and again.
pub(crate) struct RaceTestCase {
    pub(crate) description: &'static str,
    pub(crate) rounds: usize,
    /// Loads holding the table after each round.
    pub(crate) expected_winners: usize,
}
