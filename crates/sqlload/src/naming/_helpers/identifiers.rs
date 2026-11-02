use crate::naming::constants::{MAX_IDENTIFIER_LENGTH, RESERVED_WORDS};

/// Upper-case `text`, turning each `.` into `__` and every other character that is not a letter or digit into `_`.
pub(crate) fn to_identifier(text: &str) -> String {
    let mut output = String::with_capacity(text.len() + 8);
    for character in text.chars() {
        match character {
            '.' => output.push_str("__"),
            c if c.is_ascii_alphanumeric() => output.push(c.to_ascii_uppercase()),
            _ => output.push('_'),
        }
    }
    output
}

/// Why `name` cannot be an unquoted table name, if it cannot.
pub(crate) fn identifier_problem(name: &str) -> Option<String> {
    let mut chars = name.chars();
    let starts_well = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
    if !starts_well {
        return Some(format!("{name:?} must start with a letter or underscore"));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
    {
        return Some(format!(
            "{name:?} may only contain upper-case letters, digits and underscores"
        ));
    }
    if name.len() > MAX_IDENTIFIER_LENGTH {
        return Some(format!(
            "{name:?} is longer than {MAX_IDENTIFIER_LENGTH} characters"
        ));
    }
    if RESERVED_WORDS.contains(&name) {
        return Some(format!("{name:?} is a reserved word"));
    }
    None
}
