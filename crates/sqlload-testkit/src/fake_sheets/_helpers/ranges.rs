//! Query strings and A1 ranges as the Sheets API reads them.

/// A query string's pairs, `+` and `%XX` decoded.
pub(crate) fn query_pairs(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (decoded(name), decoded(value))
        })
        .collect()
}

/// `text` with `+` as a space and `%XX` as its byte.
fn decoded(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let hex = match bytes.get(index + 1..index + 3) {
            Some([high, low]) => hex_digit(*high).zip(hex_digit(*low)),
            _ => None,
        }
        .map(|(high, low)| high * 16 + low);
        match (bytes[index], hex) {
            (b'%', Some(byte)) => {
                out.push(byte);
                index += 3;
            }
            (b'+', _) => {
                out.push(b' ');
                index += 1;
            }
            (byte, _) => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// A hex digit's value.
fn hex_digit(byte: u8) -> Option<u8> {
    char::from(byte).to_digit(16).map(|digit| digit as u8)
}

/// A range's tab title (quotes undone) and its cells: the first row and column (0-based) and the last column, when it names cells.
pub(crate) fn split_range(range: &str) -> (String, Option<(usize, usize, Option<usize>)>) {
    let (tab, cells) = match range.rsplit_once('!') {
        Some((tab, cells)) => (tab, Some(cells)),
        None => (range, None),
    };
    let tab = tab
        .strip_prefix('\'')
        .and_then(|tab| tab.strip_suffix('\''))
        .map_or_else(|| tab.to_owned(), |tab| tab.replace("''", "'"));
    let cells = cells.map(|cells| {
        let (start, end) = cells.split_once(':').unwrap_or((cells, cells));
        let (start_column, start_row) = cell(start);
        let (end_column, _) = cell(end);
        (
            start_row.unwrap_or(0),
            start_column.unwrap_or(0),
            end_column,
        )
    });
    (tab, cells)
}

/// An A1 cell's column and row (0-based), either missing (`A`, `3`).
fn cell(text: &str) -> (Option<usize>, Option<usize>) {
    let letters: String = text.chars().take_while(char::is_ascii_alphabetic).collect();
    let digits = &text[letters.len()..];
    let column = (!letters.is_empty()).then(|| {
        letters
            .to_ascii_uppercase()
            .bytes()
            .fold(0, |sum, letter| sum * 26 + usize::from(letter - b'A' + 1))
            - 1
    });
    let row = (!digits.is_empty()).then(|| {
        digits
            .bytes()
            .take_while(u8::is_ascii_digit)
            .fold(0, |sum, digit| sum * 10 + usize::from(digit - b'0'))
            - 1
    });
    (column, row)
}
