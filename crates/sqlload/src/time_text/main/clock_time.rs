//! A recorded time's clock part.

/// A recorded time's clock part: `2026-11-14T07:54:11.123456Z` as `07:54:11Z`.
pub fn clock_time(recorded_at: &str) -> String {
    let time = recorded_at
        .split_once('T')
        .map_or(recorded_at, |(_, time)| time);
    let clock = time.get(..8).unwrap_or(time);
    match time.ends_with('Z') {
        true => format!("{clock}Z"),
        false => clock.to_owned(),
    }
}
