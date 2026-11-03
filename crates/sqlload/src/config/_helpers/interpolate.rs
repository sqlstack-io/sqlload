use toml::Value;

/// Replace `${NAME}` in every string with the value `lookup` returns; `$${` stays literal.
pub(crate) fn interpolate_value(
    value: &mut Value,
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<(), String> {
    match value {
        Value::String(text) => {
            *text = interpolate_text(text, lookup)?;
            Ok(())
        }
        Value::Array(items) => items
            .iter_mut()
            .try_for_each(|item| interpolate_value(item, lookup)),
        Value::Table(table) => table
            .iter_mut()
            .try_for_each(|(_, item)| interpolate_value(item, lookup)),
        _ => Ok(()),
    }
}

fn interpolate_text(text: &str, lookup: &dyn Fn(&str) -> Option<String>) -> Result<String, String> {
    let mut output = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find('$') {
        output.push_str(&rest[..start]);
        let tail = &rest[start..];
        if let Some(escaped) = tail.strip_prefix("$${") {
            output.push_str("${");
            rest = escaped;
            continue;
        }
        let Some(body) = tail.strip_prefix("${") else {
            output.push('$');
            rest = &tail[1..];
            continue;
        };
        let end = body
            .find('}')
            .ok_or_else(|| format!("unterminated ${{ in {text:?}"))?;
        let name = &body[..end];
        let resolved =
            lookup(name).ok_or_else(|| format!("environment variable {name} is not set"))?;
        output.push_str(&resolved);
        rest = &body[end + 1..];
    }
    output.push_str(rest);
    Ok(output)
}
