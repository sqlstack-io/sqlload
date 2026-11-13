//! A row id hashed from bytes.

use base64::Engine;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use shake::Shake128;
use shake::{ExtendableOutput, Update, XofReader};

use crate::row_ids::constants::ROW_ID_BYTES;

/// The base64 (standard alphabet, no padding) of the first [`ROW_ID_BYTES`] of `bytes`' SHAKE128: dlt's `digest128b`.
pub(crate) fn digest_row_id(bytes: &[u8]) -> String {
    let mut hasher = Shake128::default();
    hasher.update(bytes);
    let mut digest = [0_u8; ROW_ID_BYTES];
    hasher.finalize_xof().read(&mut digest);
    STANDARD_NO_PAD.encode(digest)
}
