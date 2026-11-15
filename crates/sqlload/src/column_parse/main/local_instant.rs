use chrono::offset::LocalResult;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;

/// The instant wall-clock time `local` is in `zone`: the earlier of the two when the clocks go back past it, and none (why not) when they go forward past it.
pub fn local_instant(local: NaiveDateTime, zone: Tz) -> Result<DateTime<Utc>, String> {
    match zone.from_local_datetime(&local) {
        LocalResult::Single(instant) | LocalResult::Ambiguous(instant, _) => Ok(instant.to_utc()),
        LocalResult::None => Err(format!(
            "a time that does not exist in {zone}, whose clocks go forward past it"
        )),
    }
}
