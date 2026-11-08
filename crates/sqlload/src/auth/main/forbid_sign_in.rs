//! Keep a run from asking anyone to sign in.

use std::sync::atomic::Ordering;

use crate::auth::_helpers::sign_in::SIGN_IN_ALLOWED;

/// From now on a browser sign-in with no usable cached tokens fails at once instead of waiting for a person, for a run whose output a program reads.
pub fn forbid_sign_in() {
    SIGN_IN_ALLOWED.store(false, Ordering::Relaxed);
}
