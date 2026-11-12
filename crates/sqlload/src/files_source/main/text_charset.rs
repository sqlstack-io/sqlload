//! Resolve a CSV source's `encoding` setting.

use crate::files_source::constants::{ASCII_LABELS, LATIN1_LABELS};
use crate::files_source::models::Charset;

/// The charset `label` names, case-insensitively: `latin-1` (and `iso-8859-1`) byte for byte, `ascii` strictly, and otherwise any label of the WHATWG Encoding Standard, such as `utf-8`, `cp1252`, `windows-1250`, `iso-8859-15`, `utf-16le`, `shift_jis` or `gbk`; an unknown label is an error.
pub fn text_charset(label: &str) -> Result<Charset, String> {
    let normal = label.trim().to_ascii_lowercase();
    if LATIN1_LABELS.contains(&normal.as_str()) {
        return Ok(Charset::Latin1);
    }
    if ASCII_LABELS.contains(&normal.as_str()) {
        return Ok(Charset::Ascii);
    }
    match encoding_rs::Encoding::for_label_no_replacement(normal.as_bytes()) {
        Some(found) if found == encoding_rs::UTF_8 => Ok(Charset::Utf8),
        Some(found) => Ok(Charset::Other(found)),
        None => Err(format!(
            "csv encoding {label:?} is not an encoding SQLLoad knows; use a label such as \"utf-8\", \"cp1252\", \"latin-1\" or \"utf-16le\""
        )),
    }
}
