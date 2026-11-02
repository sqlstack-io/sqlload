use std::collections::BTreeMap;

use regex::Regex;

/// A compiled `match` pattern: `*` is any text, `{name}` captures one or more characters other than `.`.
#[derive(Debug)]
pub(crate) struct TopicPattern {
    regex: Regex,
    pub(crate) captures: Vec<String>,
}

impl TopicPattern {
    pub(crate) fn compile(pattern: &str) -> Result<Self, String> {
        let mut source = String::from("^");
        let mut captures: Vec<String> = Vec::new();
        let mut rest = pattern;
        while !rest.is_empty() {
            if let Some(body) = rest.strip_prefix('{') {
                let end = body
                    .find('}')
                    .ok_or_else(|| format!("unterminated {{ in pattern {pattern:?}"))?;
                let name = &body[..end];
                if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                    return Err(format!(
                        "invalid capture name {name:?} in pattern {pattern:?}"
                    ));
                }
                if captures.iter().any(|existing| existing == name) {
                    return Err(format!(
                        "capture {name:?} appears twice in pattern {pattern:?}"
                    ));
                }
                source.push_str(&format!("(?P<{name}>[^.]+?)"));
                captures.push(name.to_owned());
                rest = &body[end + 1..];
            } else if let Some(after) = rest.strip_prefix('*') {
                source.push_str(".*?");
                rest = after;
            } else {
                let next = rest.find(['{', '*']).unwrap_or(rest.len());
                source.push_str(&regex::escape(&rest[..next]));
                rest = &rest[next..];
            }
        }
        source.push('$');
        let regex =
            Regex::new(&source).map_err(|error| format!("invalid pattern {pattern:?}: {error}"))?;
        Ok(Self { regex, captures })
    }

    /// The named captures when `topic` matches.
    pub(crate) fn matches(&self, topic: &str) -> Option<BTreeMap<String, String>> {
        let found = self.regex.captures(topic)?;
        let mut values: BTreeMap<String, String> = BTreeMap::new();
        for name in &self.captures {
            if let Some(value) = found.name(name) {
                values.insert(name.clone(), value.as_str().to_owned());
            }
        }
        Some(values)
    }
}

/// Fill `{name}` placeholders in `template` from `values`.
pub(crate) fn fill_template(
    template: &str,
    values: &BTreeMap<String, String>,
) -> Result<String, String> {
    let mut output = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find('{') {
        output.push_str(&rest[..start]);
        let body = &rest[start + 1..];
        let end = body
            .find('}')
            .ok_or_else(|| format!("unterminated {{ in template {template:?}"))?;
        let name = &body[..end];
        let value = values
            .get(name)
            .ok_or_else(|| format!("template {template:?} uses unknown placeholder {{{name}}}"))?;
        output.push_str(value);
        rest = &body[end + 1..];
    }
    output.push_str(rest);
    Ok(output)
}
