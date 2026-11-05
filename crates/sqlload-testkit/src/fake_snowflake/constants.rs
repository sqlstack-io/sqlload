/// The one bucket the fake serves; external stages map to its root.
pub const FAKE_BUCKET: &str = "fake-bucket";
/// The first bytes of every Parquet file.
pub const PARQUET_MAGIC: &str = "PAR1";
/// The declared type whose text the fake parses as JSON when loading Parquet.
pub const VARIANT_TYPE: &str = "VARIANT";
/// The declared type whose columns report a precision and scale.
pub const NUMBER_TYPE: &str = "NUMBER";
