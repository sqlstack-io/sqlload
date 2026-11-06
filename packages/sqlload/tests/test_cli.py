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


def copied(folder: Path) -> list[tuple[int, str]]:
	import duckdb

	with duckdb.connect(str(folder / "wh.duckdb"), read_only=True) as connection:
		return connection.execute('SELECT id, status FROM "RAW"."ORDERS" ORDER BY id').fetchall()


def test_given_no_project_when_copying_a_folder_then_its_rows_load_and_print_toml_gives_a_project_that_loads_the_same(tmp_path: Path) -> None:
	import pyarrow as pa
	import pyarrow.parquet as pq

	(tmp_path / "orders").mkdir()
	pq.write_table(pa.table({"id": [1, 2], "status": ["open", "shut"]}), tmp_path / "orders" / "part-0.parquet")
	missing = str(tmp_path / "sqlload.toml")
	done = run("copy", str(tmp_path / "orders"), f"duckdb://{tmp_path / 'wh.duckdb'}#RAW.ORDERS", "-c", missing)
	assert done.returncode == 0, done.stdout + done.stderr
	assert copied(tmp_path) == [(1, "open"), (2, "shut")]
	printed = run("copy", str(tmp_path / "orders"), f"duckdb://{tmp_path / 'again.duckdb'}#RAW.ORDERS", "--print-toml", "-c", missing)
	assert printed.returncode == 0, printed.stderr
	(tmp_path / "project").mkdir()
	(tmp_path / "project" / "sqlload.toml").write_text(printed.stdout)
	loaded = run("load", "-c", str(tmp_path / "project" / "sqlload.toml"))
	assert loaded.returncode == 0, loaded.stdout + loaded.stderr
	(tmp_path / "again.duckdb").rename(tmp_path / "wh.duckdb")
	assert copied(tmp_path) == [(1, "open"), (2, "shut")]


def test_given_a_literal_password_in_a_uri_when_the_copy_fails_then_no_output_shows_it(tmp_path: Path) -> None:
	done = run("copy", "postgresql://app:hunter2-secret@127.0.0.1:1/shop#public.accounts", f"duckdb://{tmp_path / 'wh.duckdb'}#RAW.ACCOUNTS", "-c", str(tmp_path / "sqlload.toml"))
	assert done.returncode == 1
	assert "app:***@127.0.0.1" in done.stderr, done.stderr
	assert "hunter2" not in done.stdout + done.stderr


def test_given_a_loaded_files_table_when_its_declaration_differs_then_contract_diff_reports_it_and_exits_1(tmp_path: Path) -> None:
	import pyarrow as pa
	import pyarrow.parquet as pq

	(tmp_path / "data").mkdir()
	pq.write_table(pa.table({"id": pa.array([1], pa.int64()), "status": ["open"], "amount": [1.5]}), tmp_path / "data" / "p.parquet")
	config = tmp_path / "sqlload.toml"
	config.write_text(
		f'[connections.files]\nkind = "filesystem"\nroot = "{tmp_path}"\n\n[connections.wh]\nkind = "duckdb"\ndatabase = "{tmp_path / "wh.duckdb"}"\n\n'
		'[targets.dev]\nconnection = "wh"\nloader_schema = "RAW"\n\n[sources.orders]\nkind = "files"\nconnection = "files"\npath = "data/"\ndescription = "Orders."\n'
	)
	assert "relation_missing" in run("contract", "diff", "-c", str(config)).stdout
	assert run("load", "-c", str(config)).returncode == 0
	agreed = run("contract", "diff", "-c", str(config))
	assert (agreed.returncode, agreed.stdout.splitlines()[1:]) == (0, ["  OK    table:orders", "", "0 contract difference(s)"]), agreed.stdout + agreed.stderr
	config.write_text(config.read_text() + 'contract = "enforced"\n\n[sources.orders.tables.orders]\ncolumns = { id = "int", status = "int" }\n')
	differs = run("contract", "diff", "--json", "-c", str(config))
	assert differs.returncode == 1, differs.stdout + differs.stderr
	assert [(finding["kind"], finding["column"]) for finding in json.loads(differs.stdout)["resources"][0]["findings"]] == [
		("type_mismatch", "status"),
		("missing_declaration", "amount"),
	]


def test_given_a_table_loaded_into_another_target_when_diffing_from_it_then_its_schema_there_is_read(tmp_path: Path) -> None:
	import pyarrow as pa
	import pyarrow.parquet as pq

	(tmp_path / "data").mkdir()
	pq.write_table(pa.table({"id": pa.array([1], pa.int64())}), tmp_path / "data" / "p.parquet")
	config = tmp_path / "sqlload.toml"
	targets = (
		f'[connections.files]\nkind = "filesystem"\nroot = "{tmp_path}"\n\n'
		f'[connections.dev]\nkind = "duckdb"\ndatabase = "{tmp_path / "dev.duckdb"}"\n\n'
		f'[connections.prod]\nkind = "duckdb"\ndatabase = "{tmp_path / "prod.duckdb"}"\n\n'
		'[targets.dev]\nconnection = "dev"\nloader_schema = "RAW"\n\n'
		'[targets.prod]\nconnection = "prod"\nloader_schema = "STAGE"\n\n'
		'[targets.lake]\nconnection = "files"\n\n'
	)
	source = '[sources.orders]\nkind = "files"\nconnection = "files"\npath = "data/"\ndescription = "Orders."\ndestination = "{}"\n'
	config.write_text(targets + source.format("prod"))
	assert run("load", "-c", str(config)).returncode == 0
	config.write_text(targets + source.format("dev"))
	assert "relation_missing" in run("contract", "diff", "-c", str(config)).stdout
	from_prod = run("contract", "diff", "--from", "prod", "--json", "-c", str(config))
	assert (from_prod.returncode, json.loads(from_prod.stdout)["resources"][0]["relation"]) == (0, "STAGE.orders"), from_prod.stdout + from_prod.stderr
	unknown = run("contract", "diff", "--from", "staging", "-c", str(config))
	assert (unknown.returncode, "--from staging is not a target in sqlload.toml (it has: dev, lake, prod)" in unknown.stderr) == (1, True), unknown.stdout + unknown.stderr
	from_lake = run("contract", "diff", "--from", "lake", "--json", "-c", str(config))
	assert (from_lake.returncode, json.loads(from_lake.stdout)["problems"]) == (1, ["orders: target lake writes files; contract diff compares warehouse tables (DuckDB, Snowflake) only"]), from_lake.stdout + from_lake.stderr
