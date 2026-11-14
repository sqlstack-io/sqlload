//! A projection of JSON row objects: which values load, under which column names.

use crate::row_batches::_helpers::parsed::format_problem;
use crate::row_batches::_helpers::projected::{column_name, names_field, path_tree};
use crate::row_batches::main::field_path::field_path;
use crate::row_batches::models::{FieldSpec, ProjectedField, Projection};

/// The projection `fields` and `keep_record` describe, `None` when neither is given, or why it can't be read.
pub fn projection(
    fields: Option<&[FieldSpec]>,
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
        .map(|field| {
            if let Some(problem) = field.parse.as_ref().and_then(format_problem) {
                return Err(format!("fields[{:?}]: {problem}", field.name));
            }
            Ok(ProjectedField {
                name: column_name(&field.name)?,
                path: field_path(&field.path)
                    .map_err(|reason| format!("fields[{:?}]: field path {reason}", field.name))?,
                text: field.path.clone(),
                required: field.required,
                parse: field.parse.clone(),
                on_error: field.on_error,
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
        tree: path_tree(&projected),
        fields: projected,
        keep_record,
    }))
}
