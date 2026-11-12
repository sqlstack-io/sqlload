//! The bridge process: starting it, sending it requests, and routing its replies.

use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, channel};
use std::sync::{Arc, Mutex, PoisonError};

use crate::adapters::motherduck_bridge::constants::{
    BRIDGE_SCRIPT, PYTHON_VARIABLE, STDERR_TAIL_BYTES, SUPPORTED_DUCKDB,
};
use crate::adapters::motherduck_bridge::errors::BridgeError;
use crate::adapters::motherduck_bridge::models::{BridgeProcess, Replies};

/// Start the bridge on `python`: a writer thread feeding its stdin (closing it, and reaping the process, once every handle is gone), a reader thread routing its replies and, once stdout and stderr have both closed or a reply is unreadable, failing what still waits, and one keeping its stderr's tail with each of `hidden` taken out.
pub(crate) fn start(python: &str, hidden: Vec<String>) -> Result<Arc<BridgeProcess>, BridgeError> {
    let mut child = Command::new(python)
        .args(["-c", BRIDGE_SCRIPT])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| {
            BridgeError::Setup(format!(
                "MotherDuck connects through Python's duckdb package ({SUPPORTED_DUCKDB}), and {python} cannot run ({error}): install sqlload[motherduck] with pip or uv, or set {PYTHON_VARIABLE} to a Python that has {SUPPORTED_DUCKDB}"
            ))
        })?;
    let (Some(mut stdin), Some(stdout), Some(stderr)) =
        (child.stdin.take(), child.stdout.take(), child.stderr.take())
    else {
        return Err(BridgeError::Setup(format!(
            "{python} started without its pipes"
        )));
    };
    let (lines, outgoing) = channel::<String>();
    let replies = Arc::new(Mutex::new(Replies::default()));
    let tail = Arc::new(Mutex::new(String::new()));
    std::thread::spawn(move || {
        for line in outgoing {
            if stdin
                .write_all(format!("{line}\n").as_bytes())
                .and_then(|()| stdin.flush())
                .is_err()
            {
                break;
            }
        }
        drop(stdin);
        let _ = child.wait();
    });
    let kept = tail.clone();
    let reading_stderr = std::thread::spawn(move || keep_tail(stderr, &kept, &hidden));
    let routed = replies.clone();
    let (shown, kept) = (python.to_owned(), tail.clone());
    std::thread::spawn(move || match route_replies(stdout, &routed) {
        Some(broken) => fail(
            &routed,
            format!("the MotherDuck bridge on {shown} {broken}"),
        ),
        None => {
            let _ = reading_stderr.join();
            stop(&routed, &shown, &kept);
        }
    });
    Ok(Arc::new(BridgeProcess {
        python: python.to_owned(),
        lines,
        replies,
        next_request: AtomicU64::new(1),
        next_cursor: AtomicU64::new(1),
        stderr: tail,
    }))
}

/// Send `request` with an id of its own; the receiver gets its reply, or why the bridge stopped first.
pub(crate) fn send(
    process: &BridgeProcess,
    mut request: serde_json::Value,
) -> Result<Receiver<Result<serde_json::Value, BridgeError>>, BridgeError> {
    let id = process.next_request.fetch_add(1, Ordering::Relaxed);
    request["id"] = id.into();
    let (reply, received) = channel();
    let mut replies = process
        .replies
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if let Some(stopped) = &replies.stopped {
        return Err(BridgeError::Stopped(stopped.clone()));
    }
    replies.waiting.insert(id, reply);
    drop(replies);
    if process.lines.send(request.to_string()).is_err() {
        return Err(BridgeError::Stopped(format!(
            "the MotherDuck bridge on {} is closed",
            process.python
        )));
    }
    Ok(received)
}

/// `reply`'s failure, as its message and its Python error type; `None` when it succeeded.
pub(crate) fn failure(reply: &serde_json::Value) -> Option<(String, String)> {
    if reply["ok"].as_bool() == Some(true) {
        return None;
    }
    let text = |key: &str| reply[key].as_str().unwrap_or_default().to_owned();
    Some((text("error"), text("type")))
}

/// Hand each reply line to the request waiting for it, until stdout ends (`None`) or a reply cannot be read, which says why (the bridge may still run, but no waiting request would ever get its reply).
fn route_replies(stdout: impl Read, replies: &Mutex<Replies>) -> Option<String> {
    for line in BufReader::new(stdout).lines() {
        let line = match line {
            Ok(line) => line,
            Err(error) => return Some(format!("cannot be read: {error}")),
        };
        let reply = match serde_json::from_str::<serde_json::Value>(&line) {
            Ok(reply) => reply,
            Err(error) => return Some(format!("replied with something other than JSON ({error})")),
        };
        let waiting = reply["id"].as_u64().and_then(|id| {
            replies
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .waiting
                .remove(&id)
        });
        if let Some(waiting) = waiting {
            let _ = waiting.send(Ok(reply));
        }
    }
    None
}

/// Fail every waiting and later request with why the bridge stopped, quoting the tail of its stderr.
fn stop(replies: &Mutex<Replies>, python: &str, tail: &Mutex<String>) {
    let said = tail
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .trim()
        .to_owned();
    let stopped = match said.is_empty() {
        true => format!("the MotherDuck bridge on {python} stopped"),
        false => format!("the MotherDuck bridge on {python} stopped: {said}"),
    };
    fail(replies, stopped);
}

/// Fail every waiting and later request with `stopped`.
fn fail(replies: &Mutex<Replies>, stopped: String) {
    let mut replies = replies.lock().unwrap_or_else(PoisonError::into_inner);
    for (_, waiting) in replies.waiting.drain() {
        let _ = waiting.send(Err(BridgeError::Stopped(stopped.clone())));
    }
    replies.stopped = Some(stopped);
}

/// Keep the last `STDERR_TAIL_BYTES` of `stderr`, which failures quote, with each of `hidden` taken out.
fn keep_tail(stderr: impl Read, tail: &Mutex<String>, hidden: &[String]) {
    for line in BufReader::new(stderr).lines() {
        let Ok(mut line) = line else { break };
        for secret in hidden.iter().filter(|secret| !secret.is_empty()) {
            line = line.replace(secret.as_str(), "<redacted>");
        }
        tracing::debug!(line, "MotherDuck bridge");
        let mut kept = tail.lock().unwrap_or_else(PoisonError::into_inner);
        kept.push_str(&line);
        kept.push('\n');
        if kept.len() > STDERR_TAIL_BYTES {
            let mut cut = kept.len() - STDERR_TAIL_BYTES;
            while !kept.is_char_boundary(cut) {
                cut += 1;
            }
            kept.replace_range(..cut, "");
        }
    }
}
