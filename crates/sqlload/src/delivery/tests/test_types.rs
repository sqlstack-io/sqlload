use crate::config::models::LoadErrorPolicy;
use crate::stage_api::models::LoadStatus;

pub(crate) struct ExpiredDatesTestCase {
    pub(crate) description: &'static str,
    pub(crate) retain_days: u64,
    pub(crate) lookback_days: u64,
    pub(crate) expected_dates: &'static [&'static str],
}

pub(crate) struct RenderFolderTestCase {
    pub(crate) description: &'static str,
    pub(crate) template: &'static str,
    pub(crate) expected_folder: &'static str,
}

pub(crate) struct ProfileCredentialsTestCase {
    pub(crate) description: &'static str,
    pub(crate) text: &'static str,
    pub(crate) profile: &'static str,
    /// Key id and whether a session token was found, or None when the profile has no keys.
    pub(crate) expected_keys: Option<(&'static str, bool)>,
}

pub(crate) struct PrefixTestCase {
    pub(crate) description: &'static str,
    pub(crate) reports: &'static [(&'static str, LoadStatus)],
    /// Partition and offset pairs.
    pub(crate) expected_confirmed: &'static [(i32, i64)],
    pub(crate) expected_pending_files: usize,
}

pub(crate) struct FailedLoadTestCase {
    pub(crate) description: &'static str,
    pub(crate) policy: LoadErrorPolicy,
    /// Reports for the first batch's file `a.json.gz`; `b.json.gz` always loads.
    pub(crate) reports: &'static [(&'static str, LoadStatus)],
    /// Files found missing from the stage.
    pub(crate) missing: &'static [&'static str],
    pub(crate) expected_halted: Option<&'static str>,
    pub(crate) expected_skipped: usize,
    pub(crate) expected_confirmed: &'static [(i32, i64)],
}

pub(crate) struct DueTestCase {
    pub(crate) description: &'static str,
    pub(crate) reports: &'static [(&'static str, LoadStatus)],
    /// Paths the pipe said it received before.
    pub(crate) skipped: &'static [&'static str],
    /// Seconds after sending when a check found the file in place.
    pub(crate) checked_at: &'static [i64],
    /// Seconds after sending when a check could not run.
    pub(crate) failed_at: &'static [i64],
    /// Seconds after sending when insertReport said it missed events.
    pub(crate) incomplete_at: &'static [i64],
    pub(crate) now_seconds: i64,
    pub(crate) expected_overdue: &'static [&'static str],
}

pub(crate) struct AcknowledgedTestCase {
    pub(crate) description: &'static str,
    pub(crate) skipped: &'static [&'static str],
    pub(crate) resent: &'static [&'static str],
    pub(crate) expected_acknowledged: bool,
}

pub(crate) struct ForgetTestCase {
    pub(crate) description: &'static str,
    pub(crate) revoked: &'static [i32],
    pub(crate) expected_pending_files: usize,
    pub(crate) expected_confirmed: &'static [(i32, i64)],
}

pub(crate) struct HistoryStartTestCase {
    pub(crate) description: &'static str,
    /// Paths the pipe reported as received before, for the second file.
    pub(crate) skipped: &'static [&'static str],
    pub(crate) expected_lookback_minutes: i64,
}

pub(crate) struct WarnOnceTestCase {
    pub(crate) description: &'static str,
    pub(crate) skipped: &'static [&'static str],
    pub(crate) checks: usize,
    pub(crate) expected_warnings: &'static [&'static str],
}
