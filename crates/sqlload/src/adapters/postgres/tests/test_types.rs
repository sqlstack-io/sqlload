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
