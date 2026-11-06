use crate::cdc::models::PgOutputMessage;

pub(crate) struct DecodeTestCase {
    pub(crate) description: &'static str,
    pub(crate) bytes: Vec<u8>,
    pub(crate) expected_message: Result<PgOutputMessage, &'static str>,
}
