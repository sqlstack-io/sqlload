use crate::project::models::MaskMethod;

pub(crate) struct MaskBatchTestCase {
    pub(crate) description: &'static str,
    pub(crate) method: MaskMethod,
    pub(crate) value: Option<&'static str>,
    pub(crate) expected_value: Option<&'static str>,
}
