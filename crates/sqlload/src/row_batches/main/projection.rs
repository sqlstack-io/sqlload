//! A projection of JSON row objects: which values load, under which column names.

use crate::row_batches::_helpers::projected::{column_name, names_field, path};
use crate::row_batches::models::{ProjectedField, Projection};

/// The projection `fields` (column name, path such as `$.address.city`, whether required) and `keep_record` describe, `None` when neither is given, or why it can't be read.
pub fn projection(
    fields: Option<&[(String, String, bool)]>,
    keep_record: Option<&str>,
) -> Result<Option<Projection>, String> {
    let fields = match (fields, keep_record) {
        (None, None) => return Ok(None),
        (None, Some(_)) => {
            return Err(
                "keep_record needs fields: name the columns to load beside the whole record"
                    .to_owned(),
            );
        }
        (Some(fields), _) => fields,
    };
    if fields.is_empty() && keep_record.is_none() {
        return Err(
            "fields is empty, so no column would load; name at least one, or set keep_record"
                .to_owned(),
        );
    }
    let projected = fields
        .iter()
        .map(|(name, text, required)| {
            Ok(ProjectedField {
                name: column_name(name)?,
                path: path(text)?,
                text: text.clone(),
                required: *required,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let keep_record = keep_record.map(column_name).transpose()?;
    let clash = keep_record
        .as_ref()
        .filter(|kept| names_field(&projected, kept));
    if let Some(kept) = clash {
        return Err(format!(
            "keep_record {kept:?} is also a field's name; pick another"
        ));
    }
    Ok(Some(Projection {
        fields: projected,
        keep_record,
    }))
}
