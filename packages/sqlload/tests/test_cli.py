"""The `sqlload` console script runs the native command line in-process."""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

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


@pytest.mark.parametrize(
	"config_name",
	[
		pytest.param("sqlload.toml", id="toml syntax error"),
		pytest.param("missing.toml", id="missing config file"),
	],
)
def test_given_plan_json_when_the_config_cannot_be_used_then_stdout_is_one_json_error(tmp_path: Path, config_name: str) -> None:
	(tmp_path / "sqlload.toml").write_text("[connections.wh\nkind = 'duckdb'\n")
	done = run("plan", "--json", "-c", str(tmp_path / config_name))
	assert done.returncode == 1, done.stdout + done.stderr
	assert json.loads(done.stdout)["error"]["code"] == "config", done.stdout + done.stderr


def test_given_the_version_when_imported_then_it_matches_the_package() -> None:
	assert run("--version").stdout.strip() == f"sqlload {sqlload.__version__}"


def test_given_a_command_waiting_on_the_network_when_interrupted_then_it_ends_at_once(project: Path, tmp_path: Path) -> None:
	project.write_text(project.read_text().replace('host = "localhost"', 'host = "10.255.255.1"'))
	home = tmp_path / "home"
	home.mkdir()
	env = {**os.environ, "HOME": str(home), "XDG_CONFIG_HOME": str(home), "XDG_CACHE_HOME": str(home), "XDG_DATA_HOME": str(home)}
	process = subprocess.Popen([sqlload.executable(), "debug", "-c", str(project)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
	time.sleep(2)
	if process.poll() is not None:
		pytest.skip("the unroutable address failed at once here, so there is nothing to interrupt")
	process.send_signal(signal.SIGINT)
	started = time.monotonic()
	try:
		_, stderr = process.communicate(timeout=10)
	finally:
		process.kill()
	assert time.monotonic() - started < 3
	assert process.returncode in (-signal.SIGINT, 128 + signal.SIGINT)
	assert "Traceback" not in stderr
