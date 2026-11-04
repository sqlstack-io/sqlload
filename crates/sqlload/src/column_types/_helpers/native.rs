/// The warehouse type inside `native("...")`, with its case kept; `None` if `text` is not a `native(...)` form.
pub(crate) fn native_inner(text: &str) -> Option<Result<String, String>> {
    let text = text.trim();
    let open = text.find('(')?;
    if !text[..open].trim().eq_ignore_ascii_case("native") {
        return None;
    }
    let Some(inner) = text[open + 1..].strip_suffix(')') else {
        return Some(Err(
            "has an opening parenthesis without a closing one at the end".to_owned(),
        ));
    };
    let inner = inner.trim();
    let unquoted = ['"', '\'']
        .iter()
        .find_map(|quote| inner.strip_prefix(*quote)?.strip_suffix(*quote));
    Some(match unquoted {
        Some(name) if !name.trim().is_empty() => Ok(name.trim().to_owned()),
        _ => Err("needs a quoted warehouse type, for example native(\"GEOGRAPHY\")".to_owned()),
    })
}
