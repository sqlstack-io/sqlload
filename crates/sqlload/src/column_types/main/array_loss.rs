//! What an array column loses: the array's own loss and its element's.

use std::borrow::Cow;

/// `own`, the warehouse's loss for any array, then `element`'s loss after `elements: `, joined by `; `; `None` when neither loses anything. Every warehouse words an array's loss this way.
pub fn array_loss(
    own: Option<&'static str>,
    element: Option<Cow<'static, str>>,
) -> Option<Cow<'static, str>> {
    match (own, element) {
        (Some(own), Some(element)) => Some(Cow::Owned(format!("{own}; elements: {element}"))),
        (None, Some(element)) => Some(Cow::Owned(format!("elements: {element}"))),
        (Some(own), None) => Some(Cow::Borrowed(own)),
        (None, None) => None,
    }
}
