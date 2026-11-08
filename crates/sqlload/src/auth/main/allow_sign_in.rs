//! Whether a run may ask anyone to sign in.

use std::sync::atomic::Ordering;

use crate::auth::_helpers::sign_in::SIGN_IN_ALLOWED;

/// Set for each run whether a browser sign-in with no usable cached tokens may wait for a person; a run whose output a program reads (`--json`) fails at once instead.
pub fn allow_sign_in(allowed: bool) {
    SIGN_IN_ALLOWED.store(allowed, Ordering::Relaxed);
}
