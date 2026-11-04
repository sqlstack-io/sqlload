/// A type name split into its lower-case name and its parenthesised arguments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TypeSyntax {
    /// Lower case, with runs of whitespace collapsed to one space.
    pub(crate) name: String,
    /// The comma-separated arguments, trimmed; `None` without parentheses.
    pub(crate) args: Option<Vec<String>>,
}

/// Split `text` (for example `"Decimal( 18, 2 )"`) into name and arguments.
pub(crate) fn split_type(text: &str) -> Result<TypeSyntax, String> {
    let text = text.trim();
    let Some(open) = text.find('(') else {
        return Ok(TypeSyntax {
            name: collapse(text),
            args: None,
        });
    };
    let Some(inner) = text[open + 1..].strip_suffix(')') else {
        return Err("has an opening parenthesis without a closing one at the end".to_owned());
    };
    if inner.contains(['(', ')']) {
        return Err("has nested parentheses".to_owned());
    }
    let args = if inner.trim().is_empty() {
        Vec::new()
    } else {
        inner.split(',').map(|arg| arg.trim().to_owned()).collect()
    };
    Ok(TypeSyntax {
        name: collapse(&text[..open]),
        args: Some(args),
    })
}

fn collapse(text: &str) -> String {
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}
