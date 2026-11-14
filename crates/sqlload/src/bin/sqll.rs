//! `sqll`, a short name for `sqlload`: runs the `sqlload` executable installed next to it with the same arguments, so the wheel carries one copy of the CLI.
#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

/// The `sqlload` executable's file name on this platform.
const SQLLOAD: &str = if cfg!(windows) {
    "sqlload.exe"
} else {
    "sqlload"
};

/// Run the `sqlload` beside the launcher as it was run, else beside the file a symlink to it names (macOS doesn't resolve the link), with this process's arguments.
fn main() -> ExitCode {
    let launcher = match std::env::current_exe() {
        Ok(path) => path,
        Err(error) => {
            eprintln!("sqll: cannot find where it is installed: {error}");
            return ExitCode::FAILURE;
        }
    };
    let folder = launcher.parent().map(PathBuf::from).unwrap_or_default();
    let resolved = match std::fs::canonicalize(&launcher) {
        Ok(path) => path.parent().map(|parent| parent.join(SQLLOAD)),
        Err(_) => None,
    };
    let Some(sqlload) = std::iter::once(folder.join(SQLLOAD))
        .chain(resolved)
        .find(|path| path.is_file())
    else {
        eprintln!(
            "sqll is a short name for sqlload, which isn't installed next to it ({})",
            folder.display()
        );
        return ExitCode::FAILURE;
    };
    let mut command = Command::new(&sqlload);
    command.args(std::env::args_os().skip(1));
    run(command, &sqlload)
}

/// Replace this process with `command`, so signals and the exit code are `sqlload`'s own.
#[cfg(unix)]
fn run(mut command: Command, sqlload: &Path) -> ExitCode {
    use std::os::unix::process::CommandExt;
    let error = command.exec();
    eprintln!("sqll: cannot run {}: {error}", sqlload.display());
    ExitCode::FAILURE
}

/// Run `command` with this console and stdio, and exit with its code. Ctrl-C and Ctrl-Break reach both through the shared console; while the listeners live, the console's default handler doesn't end this launcher, so `sqlload` decides what they mean and its exit code comes back.
#[cfg(not(unix))]
fn run(mut command: Command, sqlload: &Path) -> ExitCode {
    let listening = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .and_then(|runtime| {
            let _entered = runtime.enter();
            let listeners = (
                tokio::signal::windows::ctrl_c()?,
                tokio::signal::windows::ctrl_break()?,
            );
            Ok((listeners, runtime))
        });
    let _listening = match listening {
        Ok(listening) => listening,
        Err(error) => {
            eprintln!("sqll: cannot pass Ctrl-C on to sqlload: {error}");
            return ExitCode::FAILURE;
        }
    };
    match command.status() {
        Ok(status) => std::process::exit(status.code().unwrap_or(1)),
        Err(error) => {
            eprintln!("sqll: cannot run {}: {error}", sqlload.display());
            ExitCode::FAILURE
        }
    }
}
