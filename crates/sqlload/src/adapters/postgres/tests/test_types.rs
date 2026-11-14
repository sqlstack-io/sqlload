pub(crate) struct NumericTestCase {
    pub(crate) description: &'static str,
    /// The value scaled by `10^scale`.
    pub(crate) value: i128,
    pub(crate) scale: u8,
    /// Base-10000 digits, weight and sign, as Postgres sends the value.
    pub(crate) expected_digits: &'static [i16],
    pub(crate) expected_weight: i16,
    /// `0x0000` positive, `0x4000` negative.
    pub(crate) expected_sign: u16,
}

pub(crate) struct RenderedTestCase {
    pub(crate) description: &'static str,
    pub(crate) rendered: Vec<String>,
    pub(crate) expected_statements: &'static [&'static str],
}

pub(crate) struct CopyRoundTripTestCase {
    pub(crate) description: &'static str,
    /// Each column's name, the staging table's type for it, how the source's reader reads it back, and the values the batch holds.
    pub(crate) columns: Vec<(
        &'static str,
        crate::column_types::models::ColumnType,
        crate::postgres_source::models::WireType,
        arrow_array::ArrayRef,
    )>,
    pub(crate) chunk_bytes: usize,
    /// `split: true` when the stream was cut into several pieces, then each column read back by the source's reader as `name = value|value`; or the error up to its first `: `.
    pub(crate) expected_result: Result<String, &'static str>,
}

pub(crate) struct ArrayTextTestCase {
    pub(crate) description: &'static str,
    /// Each array column's name, the staging table's type for it, and the lists the batch holds.
    pub(crate) columns: Vec<(
        &'static str,
        crate::column_types::models::ColumnType,
        arrow_array::ArrayRef,
    )>,
    /// The columns staged and read back as text, or the error up to its first `: `.
    pub(crate) expected_text: Result<String, String>,
}

/// A merge's key, its columns that may be NULL, the server it runs on and the unique indexes the table has on the key, and what `plan` says of them.
pub(crate) struct NullKeysProblemTestCase {
    pub(crate) description: &'static str,
    pub(crate) nullable: &'static [&'static str],
    /// `server_version_num` and the version as Postgres prints it.
    pub(crate) version: (i32, &'static str),
    /// The table's unique indexes on the key: how many, how many treat NULLs as equal, the names of the others and the statements dropping them; `None` for a table not created yet.
    pub(crate) indexes: Option<(i64, i64, &'static str, &'static str)>,
    /// Text the problem holds; `None` when the merge can run.
    pub(crate) expected_problem: Option<&'static str>,
}
