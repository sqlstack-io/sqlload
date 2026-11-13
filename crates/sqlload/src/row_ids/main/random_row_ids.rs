//! Fresh row ids.

use base64::Engine;
use base64::engine::general_purpose::STANDARD_NO_PAD;

use crate::row_ids::constants::ROW_ID_BYTES;

/// `rows` ids of [`ROW_ID_BYTES`] random bytes each, in base64 (standard alphabet, no padding), as dlt's `uniq_id_base64`.
pub fn random_row_ids(rows: usize) -> Result<Vec<String>, String> {
    let mut bytes = vec![0_u8; ROW_ID_BYTES * rows];
    getrandom::fill(&mut bytes).map_err(|error| format!("no random bytes for row ids: {error}"))?;
    Ok(bytes
        .chunks(ROW_ID_BYTES)
        .map(|id| STANDARD_NO_PAD.encode(id))
        .collect())
}
