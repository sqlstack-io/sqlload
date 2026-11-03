pub(crate) struct EncodeRowTestCase {
    pub(crate) description: &'static str,
    pub(crate) value_type: &'static str,
    pub(crate) value: Option<&'static [u8]>,
    pub(crate) key: Option<&'static [u8]>,
    pub(crate) expected_row: &'static str,
}

pub(crate) struct UnwrapTestCase {
    pub(crate) description: &'static str,
    pub(crate) value: &'static [u8],
    pub(crate) expected_value: &'static str,
}

pub(crate) struct TimeZoneTestCase {
    pub(crate) description: &'static str,
    pub(crate) zone: chrono_tz::Tz,
    pub(crate) exported_at_ms: i64,
    pub(crate) expected_exported_at: &'static str,
}
