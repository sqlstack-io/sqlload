//! Memory sizes in binary units.

use crate::number_text::constants::{BINARY_UNIT, MEMORY_UNITS, TENTHS, WHOLE_FROM_TENTHS};

/// `bytes` in binary units to a tenth, as `512 MiB`, `1.9 GiB` or `40 KiB`; from 100 of a unit up, whole.
pub fn memory_text(bytes: u64) -> String {
    let mut unit = 0;
    let mut size: u64 = 1;
    while unit + 1 < MEMORY_UNITS.len() && bytes >= size.saturating_mul(BINARY_UNIT) {
        size *= BINARY_UNIT;
        unit += 1;
    }
    if unit == 0 {
        return format!("{bytes} bytes");
    }
    let (size, tenths_per_unit) = (u128::from(size), u128::from(TENTHS));
    let tenths = (u128::from(bytes) * tenths_per_unit + size / 2) / size;
    match tenths.is_multiple_of(tenths_per_unit) || tenths >= u128::from(WHOLE_FROM_TENTHS) {
        true => format!(
            "{} {}",
            (tenths + tenths_per_unit / 2) / tenths_per_unit,
            MEMORY_UNITS[unit]
        ),
        false => format!(
            "{}.{} {}",
            tenths / tenths_per_unit,
            tenths % tenths_per_unit,
            MEMORY_UNITS[unit]
        ),
    }
}
