use crate::time_text::constants::{FEBRUARY, MARCH_BASED_YEAR_END};

/// The UTC date as text and the milliseconds into that day.
pub(crate) fn split(unix_ms: i64) -> (String, i64) {
    let (year, month, day) = civil_from_days(unix_ms.div_euclid(86_400_000));
    (
        format!("{year:04}-{month:02}-{day:02}"),
        unix_ms.rem_euclid(86_400_000),
    )
}

/// Hours, minutes and seconds of a millisecond time of day.
pub(crate) fn clock(ms_of_day: i64) -> (i64, i64, i64) {
    let seconds = ms_of_day / 1000;
    (seconds / 3600, (seconds % 3600) / 60, seconds % 60)
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_index + 2) / 5 + 1) as u32;
    let month = if month_index < MARCH_BASED_YEAR_END {
        month_index + 3
    } else {
        month_index - 9
    } as u32;
    (
        year_of_era + era * 400 + i64::from(month <= FEBRUARY),
        month,
        day,
    )
}
