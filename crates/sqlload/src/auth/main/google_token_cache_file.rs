//! Where Google sign-ins are cached.

use std::path::PathBuf;

use crate::auth::_helpers::token_cache::token_cache_file;
use crate::auth::constants::GOOGLE_TOKEN_CACHE_FILE;

/// The Google sign-in cache file, below the user's cache folder; `None` when there is none.
pub fn google_token_cache_file() -> Option<PathBuf> {
    token_cache_file(GOOGLE_TOKEN_CACHE_FILE)
}
