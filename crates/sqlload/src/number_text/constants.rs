/// The step between binary memory units, and the units memory is shown in.
pub const BINARY_UNIT: u64 = 1024;
pub const MEMORY_UNITS: [&str; 4] = ["bytes", "KiB", "MiB", "GiB"];

/// Memory is shown to a tenth of its unit, and as a whole number from 100 of it (1,000 tenths) up.
pub const TENTHS: u64 = 10;
pub const WHOLE_FROM_TENTHS: u64 = 1_000;
