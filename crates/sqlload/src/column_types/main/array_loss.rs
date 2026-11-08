//! What an array column loses: the array's own loss and its element's.

use std::borrow::Cow;

/// `own`, the warehouse's loss for any array, followed by `element`'s loss when the element type has one.
pub fn array_loss(own: &'static str, element: Option<Cow<'static, str>>) -> Cow<'static, str> {
    match element {
        Some(element) => Cow::Owned(format!("{own}; elements: {element}")),
        None => Cow::Borrowed(own),
    }
}
