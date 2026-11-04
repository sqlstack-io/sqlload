use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, StringArray};
use arrow_schema::{DataType, Field};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::auth::models::Secret;
use crate::project::models::MaskMethod;

/// `column` with every non-null value masked by `method`; `hmac` needs `key`.
pub(crate) fn mask_column(
    field: &Field,
    column: &ArrayRef,
    method: MaskMethod,
    key: Option<&Secret>,
) -> Result<ArrayRef, String> {
    if field.data_type() != &DataType::Utf8 {
        return Err(format!(
            "masked column {} must be read as text",
            field.name()
        ));
    }
    let keyed = match (method, key) {
        (MaskMethod::Hash, _) => None,
        (MaskMethod::Hmac, Some(key)) => Some(
            Hmac::<Sha256>::new_from_slice(key.expose().as_bytes())
                .map_err(|error| error.to_string())?,
        ),
        (MaskMethod::Hmac, None) => {
            return Err(format!(
                "masked column {} uses hmac but the source has no mask_key",
                field.name()
            ));
        }
    };
    let masked: StringArray = column
        .as_string::<i32>()
        .iter()
        .map(|value| masked_value(value, keyed.as_ref()))
        .collect();
    Ok(Arc::new(masked))
}

/// The value's hex digest: HMAC-SHA256 with `keyed`, plain SHA-256 without.
fn masked_value(value: Option<&str>, keyed: Option<&Hmac<Sha256>>) -> Option<String> {
    let text = value?;
    Some(match keyed {
        Some(mac) => hex::encode(
            mac.clone()
                .chain_update(text.as_bytes())
                .finalize()
                .into_bytes(),
        ),
        None => hex::encode(Sha256::digest(text.as_bytes())),
    })
}
