/// The folder below a table's folder holding its load leases, one file per generation.
pub const LEASE_FOLDER: &str = "_sqlload/lease/";

/// How long a lease lasts after it is taken or refreshed; a load that stops without releasing it holds the table this long.
pub const LEASE_DURATION: std::time::Duration = std::time::Duration::from_secs(600);

/// How often a load refreshes its lease while it runs.
pub const LEASE_REFRESH: std::time::Duration = std::time::Duration::from_secs(60);

/// Digits in a lease generation's file name, zero-padded so names sort as the numbers do.
pub const LEASE_KEY_DIGITS: usize = 20;
