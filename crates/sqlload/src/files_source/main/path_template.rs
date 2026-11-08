//! Compile a `path_columns` template into a pattern over the keys below a files source's path.

use crate::column_types::models::{ColumnType, Warehouse};
use crate::files_source::_helpers::plan::template::segment_pattern;
use crate::files_source::constants::ANY_FOLDERS;
use crate::files_source::models::PathTemplate;

/// `text` as a pattern over whole keys below the source's path: `/` separates folders, `*` matches any text inside one segment, a `**` segment any number of folders, and `{name}` or `{name:type}` captures a value inside one segment (`text` by default; types as `destinations` take them).
pub fn path_template(text: &str, destinations: &[Warehouse]) -> Result<PathTemplate, String> {
    let fail = |reason: String| format!("path_columns {text:?}: {reason}");
    let relative = text.trim_start_matches('/');
    if relative.is_empty() {
        return Err(fail("the template is empty".to_owned()));
    }
    if relative.ends_with('/') {
        return Err(fail(
            "it must match whole file keys, so it cannot end in /; end it with * to match any file name"
                .to_owned(),
        ));
    }
    let segments: Vec<&str> = relative.split('/').collect();
    let mut pattern = String::from("^");
    let mut captures: Vec<(String, ColumnType)> = Vec::new();
    for (index, segment) in segments.iter().enumerate() {
        let last = index + 1 == segments.len();
        if *segment == ANY_FOLDERS {
            pattern.push_str(match last {
                true => ".*",
                false => "(?:[^/]+/)*",
            });
            continue;
        }
        if segment.is_empty() {
            return Err(fail("it has an empty folder (//)".to_owned()));
        }
        let (segment_regex, found) = segment_pattern(segment, destinations).map_err(fail)?;
        for (name, column_type) in found {
            if captures.iter().any(|(seen, _)| *seen == name) {
                return Err(fail(format!("{name} is captured twice")));
            }
            captures.push((name, column_type));
        }
        pattern.push_str(&segment_regex);
        if !last {
            pattern.push('/');
        }
    }
    if captures.is_empty() {
        return Err(fail(
            "it captures nothing; name a value with {column} or {column:type}".to_owned(),
        ));
    }
    pattern.push('$');
    let pattern = regex::Regex::new(&pattern).map_err(|error| fail(error.to_string()))?;
    Ok(PathTemplate {
        text: text.to_owned(),
        pattern,
        captures,
    })
}
