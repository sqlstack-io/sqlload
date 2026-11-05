use crate::stage_api::models::LoadStatus;

pub(crate) struct EncryptionTestCase {
    pub(crate) description: &'static str,
    pub(crate) key_bytes: usize,
    pub(crate) plaintext_bytes: usize,
    pub(crate) expected_ciphertext_bytes: usize,
    pub(crate) expected_matdesc: &'static str,
}

pub(crate) struct SignTestCase {
    pub(crate) description: &'static str,
    pub(crate) method: &'static str,
    pub(crate) uri: &'static str,
    pub(crate) headers: &'static [(&'static str, &'static str)],
    pub(crate) payload_hash: &'static str,
    pub(crate) expected_signature: &'static str,
}

pub(crate) struct StageResponseTestCase {
    pub(crate) description: &'static str,
    pub(crate) response: &'static str,
    pub(crate) expected_host: &'static str,
    pub(crate) expected_path: &'static str,
}

pub(crate) struct StageResponseErrorTestCase {
    pub(crate) description: &'static str,
    pub(crate) response: &'static str,
    pub(crate) expected_error: &'static str,
}

pub(crate) struct LoadReportTestCase {
    pub(crate) description: &'static str,
    pub(crate) response: &'static str,
    /// Path, status and first error of each file.
    pub(crate) expected_files: &'static [(&'static str, LoadStatus, Option<&'static str>)],
    pub(crate) expected_begin_mark: Option<&'static str>,
    pub(crate) expected_complete: bool,
}

pub(crate) struct SequenceIdsTestCase {
    pub(crate) description: &'static str,
    pub(crate) threads: usize,
    pub(crate) each: usize,
    pub(crate) expected_unique: usize,
}
