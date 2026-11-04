//! Fill `{placeholder}`s in a path template.

use crate::store_paths::errors::PathError;

/// `template` with each `{name}` replaced by its value; names must be in `values`, values cannot contain `/`.
pub fn render_template(template: &str, values: &[(&str, &str)]) -> Result<String, PathError> {
    let mut output = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        output.push_str(&rest[..open]);
        let Some(close) = rest[open..].find('}') else {
            return Err(PathError::UnclosedPlaceholder {
                template: template.to_owned(),
            });
        };
        let name = &rest[open + 1..open + close];
        let Some((_, value)) = values.iter().find(|(known, _)| *known == name) else {
            return Err(PathError::UnknownPlaceholder {
                template: template.to_owned(),
                placeholder: name.to_owned(),
                known: values
                    .iter()
                    .map(|(known, _)| format!("{{{known}}}"))
                    .collect::<Vec<_>>()
                    .join(", "),
            });
        };
        if value.contains('/') {
            return Err(PathError::SlashInValue {
                placeholder: name.to_owned(),
                value: (*value).to_owned(),
            });
        }
        output.push_str(value);
        rest = &rest[open + close + 1..];
    }
    output.push_str(rest);
    Ok(output)
}
