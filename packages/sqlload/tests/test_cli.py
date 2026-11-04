"""The `sqlload` console script runs the native command line in-process."""

import subprocess
import sys
from pathlib import Path

import sqlload


def run(*args: str) -> subprocess.CompletedProcess[str]:
	return subprocess.run([sys.executable, "-c", "from sqlload._cli import main; main()", *args], capture_output=True, text=True)


def test_given_help_when_run_then_the_native_commands_are_listed() -> None:
	done = run("--help")
	assert done.returncode == 0
	assert "debug" in done.stdout and "load" in done.stdout


def test_given_a_project_when_debugging_without_connections_then_it_validates(project: Path) -> None:
	done = run("debug", "--no-connection", "-c", str(project))
	assert done.returncode == 0, done.stderr


def test_given_a_bad_flag_when_run_then_the_usage_error_exits_2() -> None:
	done = run("--no-such-flag")
	assert done.returncode == 2
	assert "unexpected argument" in done.stderr


def test_given_the_version_when_imported_then_it_matches_the_package() -> None:
	assert run("--version").stdout.strip() == f"sqlload {sqlload.__version__}"
