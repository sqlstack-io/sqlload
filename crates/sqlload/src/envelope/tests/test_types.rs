pub(crate) struct EncodeRowTestCase {
    pub(crate) description: &'static str,
    pub(crate) value_type: &'static str,
    pub(crate) value: Option<&'static [u8]>,
    pub(crate) key: Option<&'static [u8]>,
    pub(crate) expected_row: &'static str,
}
