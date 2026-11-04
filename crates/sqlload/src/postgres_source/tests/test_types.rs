use crate::column_types::models::ColumnType;
use crate::postgres_source::models::PgType;

pub(crate) struct ReadNumericTestCase {
    pub(crate) description: &'static str,
    /// Base-10000 digits, weight, sign word and display scale.
    pub(crate) digits: &'static [i16],
    pub(crate) weight: i16,
    pub(crate) sign: u16,
    pub(crate) precision: u8,
    pub(crate) scale: u8,
    /// The scaled integer, or the error text.
    pub(crate) expected_result: Result<i128, &'static str>,
}

pub(crate) struct CopyDecoderTestCase {
    pub(crate) description: &'static str,
    /// Bytes fed to the decoder per push.
    pub(crate) chunk_size: usize,
    pub(crate) expected_rows: usize,
}

pub(crate) struct ReadColumnTestCase {
    pub(crate) description: &'static str,
    pub(crate) pg_type: PgType,
    pub(crate) output: ColumnType,
    /// The select expression, or the error text.
    pub(crate) expected_result: Result<&'static str, &'static str>,
}
