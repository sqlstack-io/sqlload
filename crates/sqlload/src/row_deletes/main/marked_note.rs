//! What a key scan did, for the table's line in the tally.

use crate::number_text::main::thousands::thousands;

/// What a key scan reading `keys` keys did, marking `marked` rows (`soft`: setting `_sqlload_deleted_at`, else removing them): `(key scan of 4,812 keys: 3 rows soft-deleted)`.
pub fn marked_note(soft: bool, (keys, marked): (u64, u64)) -> String {
    let rows = match marked {
        1 => "1 row".to_owned(),
        _ => format!("{} rows", thousands(marked)),
    };
    let action = match soft {
        true => "soft-deleted",
        false => "deleted",
    };
    format!("(key scan of {} keys: {rows} {action})", thousands(keys))
}
