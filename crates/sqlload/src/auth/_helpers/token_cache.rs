//! The file browser sign-ins are cached in, readable only by its owner, keyed by account, user, OAuth client and role.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::auth::constants::TOKEN_CACHE_FILE;
use crate::auth::models::{CachedTokens, OauthClient, OauthTokens, Secret};

/// The token cache file: below `XDG_CACHE_HOME`, else `~/.cache` (`%LOCALAPPDATA%` on Windows).
pub(crate) fn token_cache_file() -> Option<PathBuf> {
    let variable = |name: &str| {
        std::env::var_os(name)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
    };
    let folder = match std::env::consts::OS {
        "windows" => variable("LOCALAPPDATA"),
        _ => {
            variable("XDG_CACHE_HOME").or_else(|| variable("HOME").map(|home| home.join(".cache")))
        }
    };
    folder.map(|folder| folder.join(TOKEN_CACHE_FILE))
}

/// The key one sign-in is cached under.
pub(crate) fn cache_key(client: &OauthClient) -> String {
    format!(
        "{} {} {} {}",
        client.account_url,
        client.user.to_ascii_uppercase(),
        client.client_id,
        client
            .role
            .as_deref()
            .unwrap_or_default()
            .to_ascii_uppercase()
    )
}

/// The cached sign-in for `client`; none when the file is missing or unreadable.
pub(crate) fn read_cached(client: &OauthClient) -> Option<OauthTokens> {
    let cached = read_file(&client.cache_file).remove(&cache_key(client))?;
    Some(OauthTokens {
        access_token: Secret(cached.access_token),
        access_expires_at: cached.access_expires_at,
        refresh_token: cached.refresh_token.map(Secret),
        refresh_expires_at: cached.refresh_expires_at,
    })
}

/// Cache `tokens` for `client`, keeping other sign-ins in the file: written to a new file only its owner can read, then renamed over the old one.
pub(crate) fn write_cached(client: &OauthClient, tokens: &OauthTokens) -> Result<(), String> {
    let path = &client.cache_file;
    let mut all = read_file(path);
    all.insert(
        cache_key(client),
        CachedTokens {
            access_token: tokens.access_token.expose().to_owned(),
            access_expires_at: tokens.access_expires_at,
            refresh_token: tokens
                .refresh_token
                .as_ref()
                .map(|token| token.expose().to_owned()),
            refresh_expires_at: tokens.refresh_expires_at,
        },
    );
    let text = serde_json::to_string_pretty(&all).map_err(|error| error.to_string())?;
    let folder = path
        .parent()
        .ok_or_else(|| format!("{} has no folder", path.display()))?;
    create_private_folder(folder)
        .map_err(|error| format!("cannot create {}: {error}", folder.display()))?;
    let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
    write_private(&temporary, text.as_bytes())
        .and_then(|()| std::fs::rename(&temporary, path))
        .map_err(|error| {
            let _ = std::fs::remove_file(&temporary);
            format!("cannot write {}: {error}", path.display())
        })
}

/// Every cached sign-in in `path`; empty when the file is missing, and with a warning when it cannot be read.
fn read_file(path: &Path) -> BTreeMap<String, CachedTokens> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return BTreeMap::new(),
        Err(error) => {
            tracing::warn!(%error, file = %path.display(), "cannot read the Snowflake sign-in cache");
            return BTreeMap::new();
        }
    };
    serde_json::from_str(&text).unwrap_or_else(|error| {
        tracing::warn!(%error, file = %path.display(), "the Snowflake sign-in cache is not valid; signing in again");
        BTreeMap::new()
    })
}

/// Create `folder` (and its parents), readable only by its owner on Unix.
fn create_private_folder(folder: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(folder)
    }
    #[cfg(not(unix))]
    {
        std::fs::create_dir_all(folder)
    }
}

/// Write a new file only its owner can read and write (0600 on Unix).
fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}
