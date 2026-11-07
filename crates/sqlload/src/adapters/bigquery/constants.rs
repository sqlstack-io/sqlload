/// The most digits before the point a parameterised `NUMERIC` holds; more need `BIGNUMERIC`.
pub const NUMERIC_MAX_INTEGER_DIGITS: u8 = 29;

/// The most digits after the point a parameterised `NUMERIC` holds; more need `BIGNUMERIC`.
pub const NUMERIC_MAX_SCALE: u8 = 9;

/// The precision and scale BigQuery gives a `NUMERIC` declared without them.
pub const NUMERIC_DEFAULT: (u8, u8) = (38, 9);
