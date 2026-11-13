//! dlt 1.32.0's default naming convention (`snake_case`), step by step.

use base64::Engine;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use shake::Shake128;
use shake::{ExtendableOutput, Update, XofReader};
use unicode_properties::{GeneralCategory, UnicodeGeneralCategory};

use crate::load_naming::constants::{DLT_SWAPPED, DLT_TAG_BYTES};

/// `identifier` (already trimmed) in dlt's snake_case, before shortening: swapped characters, every run of other characters but ASCII letters, digits and `_` as one `_`, words split at case changes, lower case, `_` before a leading digit, each trailing `_` as `x`, and runs of `_` as one.
pub(crate) fn snake_case(identifier: &str) -> String {
    let mut kept: Vec<char> = Vec::with_capacity(identifier.len());
    let mut replacing = false;
    for character in identifier.chars() {
        let character = DLT_SWAPPED
            .iter()
            .find(|(from, _)| *from == character)
            .map_or(character, |(_, to)| *to);
        let allowed = character.is_ascii_alphabetic() || character == '_' || is_digit(character);
        if allowed {
            kept.push(character);
        } else if !replacing {
            kept.push('_');
        }
        replacing = !allowed;
    }
    let mut words = case_breaks(&title_breaks(&kept));
    if words.first().copied().is_some_and(is_digit) {
        words.insert(0, '_');
    }
    let trailing = words
        .iter()
        .rev()
        .take_while(|character| **character == '_')
        .count();
    words.truncate(words.len() - trailing);
    words.extend(std::iter::repeat_n('x', trailing));
    let mut snake = String::with_capacity(words.len());
    for character in words {
        if !(character == '_' && snake.ends_with('_')) {
            snake.push(character);
        }
    }
    snake
}

/// `normalized` cut to `max_length` characters around the tag of `original`, as dlt's `shorten_identifier` cuts it: the head and tail kept, the head a character longer when they cannot be even; `normalized` itself when it fits or there is no limit.
pub(crate) fn shortened(normalized: String, original: &str, max_length: Option<usize>) -> String {
    let characters: Vec<char> = normalized.chars().collect();
    let Some(max_length) = max_length.filter(|max| *max > 0 && characters.len() > *max) else {
        return normalized;
    };
    let mut hasher = Shake128::default();
    hasher.update(original.as_bytes());
    let mut digest = [0_u8; DLT_TAG_BYTES];
    hasher.finalize_xof().read(&mut digest);
    let tag = STANDARD_NO_PAD
        .encode(digest)
        .replace('/', "a")
        .replace('+', "b")
        .to_ascii_lowercase();
    let remaining = max_length.saturating_sub(tag.len());
    let head: String = characters[..remaining / 2 + remaining % 2].iter().collect();
    let tail: String = characters[characters.len() - remaining / 2..]
        .iter()
        .collect();
    format!("{head}{tag}{tail}")
}

/// Whether Python's `\d` matches `character`: a decimal digit (Unicode's `Nd`) of any script.
fn is_digit(character: char) -> bool {
    character.is_ascii_digit() || character.general_category() == GeneralCategory::DecimalNumber
}

/// `_` put before each capitalised word (an upper-case letter and the lower-case ones after it) that follows anything but `_`, matched left to right without overlap, as `re.sub("([^_])([A-Z][a-z]+)", r"\1_\2")` does.
fn title_breaks(characters: &[char]) -> Vec<char> {
    let mut broken = Vec::with_capacity(characters.len() + characters.len() / 2);
    let mut index = 0;
    while index < characters.len() {
        let character = characters[index];
        broken.push(character);
        let lower = characters
            .get(index + 2..)
            .unwrap_or_default()
            .iter()
            .take_while(|next| next.is_ascii_lowercase())
            .count();
        let capital = characters
            .get(index + 1)
            .is_some_and(char::is_ascii_uppercase);
        if character != '_' && capital && lower > 0 {
            broken.push('_');
            broken.extend_from_slice(&characters[index + 1..index + 2 + lower]);
            index += 2 + lower;
        } else {
            index += 1;
        }
    }
    broken
}

/// `_` put between a lower-case letter or ASCII digit and the upper-case letter after it, then everything in lower case, as `re.sub("([a-z0-9])([A-Z])", r"\1_\2").lower()` does.
fn case_breaks(characters: &[char]) -> Vec<char> {
    let mut broken = Vec::with_capacity(characters.len() + characters.len() / 2);
    for (index, character) in characters.iter().enumerate() {
        broken.push(character.to_ascii_lowercase());
        let before = character.is_ascii_lowercase() || character.is_ascii_digit();
        if before
            && characters
                .get(index + 1)
                .is_some_and(char::is_ascii_uppercase)
        {
            broken.push('_');
        }
    }
    broken
}
