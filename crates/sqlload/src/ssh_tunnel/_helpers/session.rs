//! Signing in to an SSH server (a bastion, or an SFTP server) and checking who it is.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use russh::client::{Config, Handle};
use russh::keys::agent::client::AgentClient;
use russh::keys::known_hosts::known_host_keys_path;
use russh::keys::{HashAlg, PrivateKeyWithHashAlg, PublicKeyOrCertificate, load_secret_key};

#[cfg(windows)]
use crate::ssh_tunnel::constants::OPENSSH_AGENT_PIPE;
use crate::ssh_tunnel::constants::{CONNECT_TIMEOUT, KEEPALIVE_INTERVAL, KEEPALIVE_MAX};
use crate::ssh_tunnel::models::{HostCheck, SshTunnel};

/// One bastion's session, opened by one runtime; locked while it opens, so connections to other bastions don't wait on it.
type Slot = Arc<tokio::sync::Mutex<Option<Arc<Handle<HostCheck>>>>>;

/// The sessions, one per runtime, bastion and sign-in. A session's task runs on the runtime that opened it and ends with it, so two runs in one process (Python threads, say) never share one, and each run closes its own.
static SESSIONS: OnceLock<Mutex<HashMap<String, Slot>>> = OnceLock::new();

/// The tunnels, by [`slot_key`], whose connections saw their session close: a session can be reopened (by another table) before a failed connection asks, so the closing is kept for the rest of the run.
static DROPPED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

/// Which session `tunnel` uses on the current runtime: the runtime, the server and the sign-in (a password by its hash, so two passwords never share a session and neither is kept as text).
pub(crate) fn slot_key(tunnel: &SshTunnel) -> String {
    format!(
        "{}|{}|{:?}|{:?}|{:?}",
        tokio::runtime::Handle::current().id(),
        tunnel.route(),
        tunnel.private_key_file,
        tunnel.password.as_ref().map(|password| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            std::hash::Hash::hash(password.expose(), &mut hasher);
            std::hash::Hasher::finish(&hasher)
        }),
        tunnel.known_hosts
    )
}

/// The slot for `key`, after dropping other slots whose sessions closed (their runs ended); a slot someone holds (copies are only made under the lock) stays.
fn slot(key: &str) -> Slot {
    let mut slots = SESSIONS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    slots.retain(|other, slot| {
        other == key
            || Arc::strong_count(slot) > 1
            || slot.try_lock().map_or(true, |session| {
                session.as_ref().is_some_and(|open| !open.is_closed())
            })
    });
    slots.entry(key.to_owned()).or_default().clone()
}

/// The open session with the bastion of `tunnel`, opening one when there is none or it closed.
pub(crate) async fn shared_session(tunnel: &SshTunnel) -> Result<Arc<Handle<HostCheck>>, String> {
    let slot = slot(&slot_key(tunnel));
    let mut session = slot.lock().await;
    if let Some(open) = session.as_ref().filter(|open| !open.is_closed()) {
        return Ok(open.clone());
    }
    let open = Arc::new(open_session(tunnel).await?);
    *session = Some(open.clone());
    Ok(open)
}

/// Note that a connection through the tunnel `key` saw its session close.
pub(crate) fn mark_dropped(key: &str) {
    DROPPED
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(key.to_owned());
}

/// Whether a connection through `tunnel` on this run saw its session close: a connection through it that failed failed because the tunnel went.
pub(crate) fn session_closed(tunnel: &SshTunnel) -> bool {
    DROPPED
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains(&slot_key(tunnel))
}

/// `path` with a leading `~/` read as the home folder.
pub(crate) fn expanded(path: &Path) -> PathBuf {
    match (path.strip_prefix("~"), std::env::var_os("HOME")) {
        (Ok(rest), Some(home)) => PathBuf::from(home).join(rest),
        _ => path.to_path_buf(),
    }
}

/// Why the bastion's host key is not trusted, with its fingerprint and the fix; `None` when `known_hosts` lists it.
pub(crate) fn host_key_refusal(check: &HostCheck, key: &PublicKeyOrCertificate) -> Option<String> {
    let PublicKeyOrCertificate::PublicKey { key, .. } = key else {
        return Some("the server presented a host certificate; SQLLoad checks plain host keys against known_hosts only".to_owned());
    };
    let shown = check.known_hosts.display();
    let fingerprint = key.fingerprint(HashAlg::Sha256);
    let listed = match check.port {
        22 => check.host.clone(),
        port => format!("[{}]:{port}", check.host),
    };
    if !check.known_hosts.is_file() {
        return Some(format!(
            "{shown} is missing or not a file, so the server's host key {fingerprint} ({}) cannot be checked; once its administrator confirms that fingerprint, add it with `ssh-keyscan -p {} {} >> {shown}` and compare the fingerprint it prints",
            key.algorithm(),
            check.port,
            check.host,
        ));
    }
    let listed_keys = match known_host_keys_path(&check.host, check.port, &check.known_hosts) {
        Ok(keys) => keys,
        Err(error) => return Some(format!("cannot read {shown}: {error}")),
    };
    match (
        listed_keys.iter().any(|(_, listed_key)| listed_key == key),
        listed_keys
            .iter()
            .any(|(_, listed_key)| listed_key.algorithm() == key.algorithm()),
    ) {
        (true, _) => None,
        (false, true) => Some(format!(
            "the server's host key changed: it now presents {fingerprint} ({}), which {shown} does not list for {listed}; if the change is expected, remove the old entry with `ssh-keygen -R '{listed}' -f {shown}`, then add the new one with `ssh-keyscan -p {} {} >> {shown}` and compare the fingerprint it prints",
            key.algorithm(),
            check.port,
            check.host,
        )),
        (false, false) => Some(format!(
            "the server's host key {fingerprint} ({}) is not in {shown}; once its administrator confirms that fingerprint, add it with `ssh-keyscan -p {} {} >> {shown}` and compare the fingerprint it prints",
            key.algorithm(),
            check.port,
            check.host,
        )),
    }
}

/// A signed-in session with the bastion of `tunnel`, sending keepalives.
async fn open_session(tunnel: &SshTunnel) -> Result<Handle<HostCheck>, String> {
    let config = Config {
        keepalive_interval: Some(KEEPALIVE_INTERVAL),
        keepalive_max: KEEPALIVE_MAX,
        nodelay: true,
        ..Config::default()
    };
    let refusal = Arc::new(Mutex::new(None));
    let check = HostCheck {
        host: tunnel.host.clone(),
        port: tunnel.port(),
        known_hosts: expanded(
            tunnel
                .known_hosts
                .as_deref()
                .unwrap_or(Path::new("~/.ssh/known_hosts")),
        ),
        refusal: refusal.clone(),
    };
    let connected = tokio::time::timeout(
        CONNECT_TIMEOUT,
        russh::client::connect(
            Arc::new(config),
            (tunnel.host.as_str(), tunnel.port()),
            check,
        ),
    )
    .await;
    let session = match connected {
        Err(_) => return Err(format!("no answer within {}s", CONNECT_TIMEOUT.as_secs())),
        Ok(Ok(session)) => session,
        Ok(Err(error)) => {
            let refused = refusal
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            return Err(refused.unwrap_or_else(|| error.to_string()));
        }
    };
    tokio::time::timeout(CONNECT_TIMEOUT, sign_in(session, tunnel))
        .await
        .map_err(|_| format!("signing in took longer than {}s", CONNECT_TIMEOUT.as_secs()))?
}

/// `session` signed in as `tunnel.user` with its private key, else its password, else ssh-agent's keys.
async fn sign_in(
    mut session: Handle<HostCheck>,
    tunnel: &SshTunnel,
) -> Result<Handle<HostCheck>, String> {
    let rsa_hash = session
        .best_supported_rsa_hash()
        .await
        .map_err(|error| error.to_string())?
        .flatten();
    let user = tunnel.user.as_str();
    let Some(file) = &tunnel.private_key_file else {
        return match &tunnel.password {
            Some(password) => password_sign_in(session, user, password.expose()).await,
            None => agent_sign_in(session, user, rsa_hash).await,
        };
    };
    let file = expanded(file);
    let passphrase = tunnel
        .private_key_passphrase
        .as_ref()
        .map(|secret| secret.expose());
    let key = load_secret_key(&file, passphrase)
        .map_err(|error| format!("cannot read private key {}: {error}", file.display()))?;
    let result = session
        .authenticate_publickey(user, PrivateKeyWithHashAlg::new(Arc::new(key), rsa_hash))
        .await
        .map_err(|error| error.to_string())?;
    match result.success() {
        true => Ok(session),
        false => Err(format!(
            "the server refused {user}'s key {}",
            file.display()
        )),
    }
}

/// `session` signed in as `user` with `password`.
async fn password_sign_in(
    mut session: Handle<HostCheck>,
    user: &str,
    password: &str,
) -> Result<Handle<HostCheck>, String> {
    let result = session
        .authenticate_password(user, password)
        .await
        .map_err(|error| error.to_string())?;
    match result.success() {
        true => Ok(session),
        false => Err(format!("the server refused {user}'s password")),
    }
}

/// ssh-agent: the socket `SSH_AUTH_SOCK` names.
#[cfg(unix)]
async fn connect_agent() -> Result<AgentClient<tokio::net::UnixStream>, russh::keys::Error> {
    AgentClient::connect_env().await
}

/// ssh-agent on Windows: the pipe `SSH_AUTH_SOCK` names, or else the OpenSSH agent service's.
#[cfg(windows)]
async fn connect_agent()
-> Result<AgentClient<tokio::net::windows::named_pipe::NamedPipeClient>, russh::keys::Error> {
    let pipe = std::env::var("SSH_AUTH_SOCK").unwrap_or_else(|_| OPENSSH_AGENT_PIPE.to_owned());
    AgentClient::connect_named_pipe(pipe).await
}

/// `session` signed in as `user` with the first key ssh-agent holds that the bastion accepts.
async fn agent_sign_in(
    mut session: Handle<HostCheck>,
    user: &str,
    rsa_hash: Option<HashAlg>,
) -> Result<Handle<HostCheck>, String> {
    let mut agent = connect_agent().await.map_err(|error| {
        format!("neither private_key_file nor password is set and ssh-agent is not reachable ({error}); set private_key_file or password")
    })?;
    let identities = agent
        .request_identities()
        .await
        .map_err(|error| format!("cannot list ssh-agent's keys: {error}"))?;
    for identity in &identities {
        let accepted = session
            .authenticate_publickey_with(
                user,
                identity.public_key().into_owned(),
                rsa_hash,
                &mut agent,
            )
            .await
            .map_err(|error| format!("signing in with ssh-agent's keys failed: {error}"))?;
        if accepted.success() {
            return Ok(session);
        }
    }
    Err(format!(
        "the server refused {user} with each of ssh-agent's {} keys; set private_key_file or password",
        identities.len()
    ))
}
