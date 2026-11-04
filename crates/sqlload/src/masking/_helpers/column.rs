use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, StringArray};
use arrow_schema::{DataType, Field};
use sha2::{Digest, Sha256};

use crate::project::models::MaskMethod;

/// `column` with every non-null value masked by `method`.
pub(crate) fn mask_column(
    field: &Field,
    column: &ArrayRef,
    method: MaskMethod,
) -> Result<ArrayRef, String> {
    if field.data_type() != &DataType::Utf8 {
        return Err(format!(
            "masked column {} must be read as text",
            field.name()
        ));
    }
    let masked: StringArray = column
        .as_string::<i32>()
        .iter()
        .map(|value| masked_value(value, method))
        .collect();
    Ok(Arc::new(masked))
}

fn masked_value(value: Option<&str>, method: MaskMethod) -> Option<String> {
    let text = value?;
    Some(match method {
        MaskMethod::Hash => hex::encode(Sha256::digest(text.as_bytes())),
    })
}
