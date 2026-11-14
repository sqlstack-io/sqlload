"""The `sqlload` console script runs the native command line in-process."""

import json
import os
import signal
import subprocess
import sys
import sysconfig
import time
from pathlib import Path

import pytest

import sqlload
from sqlload import _cli


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


@pytest.mark.parametrize(
	("args", "expected_code"),
	[
		pytest.param(("--version",), 0, id="version"),
		pytest.param(("debug", "--no-connection", "-c", "PROJECT"), 0, id="a command on a project"),
		pytest.param(("plan", "--json", "-c", "MISSING"), 1, id="a failing plan"),
		pytest.param(("--no-such-flag",), 2, id="a usage error"),
	],
)
def test_given_the_sqll_script_when_run_then_it_is_the_sqlload_cli_under_a_short_name(project: Path, tmp_path: Path, args: tuple[str, ...], expected_code: int) -> None:
	scripts = Path(sysconfig.get_path("scripts"))
	named = {"PROJECT": str(project), "MISSING": str(tmp_path / "missing.toml")}
	command = [named.get(arg, arg) for arg in args]
	runs = [subprocess.run([str(scripts / name), *command], capture_output=True, text=True) for name in ("sqlload", "sqll")]
	assert (runs[1].returncode, runs[1].stdout, runs[1].stderr) == (runs[0].returncode, runs[0].stdout, runs[0].stderr)
	assert runs[1].returncode == expected_code, runs[1].stdout + runs[1].stderr


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
	assert done.returncode == 75, "a refused connection is worth retrying"
	assert "app:***@127.0.0.1" in done.stderr, done.stderr
	assert "cannot connect to Postgres at 127.0.0.1:1/shop" in done.stdout, done.stdout
	assert "hunter2" not in done.stdout + done.stderr


def test_given_named_connections_when_print_toml_is_appended_to_their_project_then_it_loads_the_table(tmp_path: Path) -> None:
	import pyarrow as pa
	import pyarrow.parquet as pq

	(tmp_path / "orders").mkdir()
	pq.write_table(pa.table({"id": [1, 2], "status": ["open", "shut"]}), tmp_path / "orders" / "part-0.parquet")
	config = tmp_path / "sqlload.toml"
	config.write_text(
		f'default_target = "dev"\n\n[connections.files]\nkind = "filesystem"\nroot = "{tmp_path / "orders"}"\n\n'
		f'[connections.wh]\nkind = "duckdb"\ndatabase = "{tmp_path / "wh.duckdb"}"\n\n[targets.dev]\nconnection = "wh"\nloader_schema = "RAW"\n'
	)
	printed = run("copy", "files:", "wh:RAW.ORDERS", "--print-toml", "-c", str(config))
	assert printed.returncode == 0, printed.stderr
	assert "[connections." not in printed.stdout and "default_target" not in printed.stdout, printed.stdout
	config.write_text(config.read_text() + "\n" + printed.stdout)
	loaded = run("load", "-c", str(config))
	assert loaded.returncode == 0, loaded.stdout + loaded.stderr
	assert copied(tmp_path) == [(1, "open"), (2, "shut")]


def test_given_a_loaded_files_table_when_its_declaration_differs_then_contract_diff_reports_it_and_exits_1(tmp_path: Path) -> None:
	import pyarrow as pa
	import pyarrow.parquet as pq

	(tmp_path / "data").mkdir()
	pq.write_table(pa.table({"id": pa.array([1], pa.int64()), "status": ["open"], "amount": [1.5]}), tmp_path / "data" / "p.parquet")
	config = tmp_path / "sqlload.toml"
	config.write_text(
		f'[connections.files]\nkind = "filesystem"\nroot = "{tmp_path}"\n\n[connections.wh]\nkind = "duckdb"\ndatabase = "{tmp_path / "wh.duckdb"}"\n\n'
		'[targets.dev]\nconnection = "wh"\nloader_schema = "RAW"\n\n[sources.orders]\nwrite_strategy = "replace"\nkind = "files"\nconnection = "files"\npath = "data/"\ndescription = "Orders."\n'
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


def test_given_new_files_of_other_columns_when_diffing_with_source_then_each_undeclared_change_is_reported(tmp_path: Path) -> None:
	import pyarrow as pa
	import pyarrow.parquet as pq

	(tmp_path / "data").mkdir()
	pq.write_table(pa.table({"id": pa.array([1], pa.int64()), "code": ["a"], "qty": pa.array([2], pa.int64()), "gone": [True]}), tmp_path / "data" / "p.parquet")
	config = tmp_path / "sqlload.toml"
	config.write_text(
		f'[connections.files]\nkind = "filesystem"\nroot = "{tmp_path}"\n\n[connections.wh]\nkind = "duckdb"\ndatabase = "{tmp_path / "wh.duckdb"}"\n\n'
		'[targets.dev]\nconnection = "wh"\nloader_schema = "RAW"\n\n[sources.orders]\nwrite_strategy = "replace"\nkind = "files"\nconnection = "files"\npath = "data/"\ndescription = "Orders."\n'
		'\n[sources.orders.tables.orders]\ncolumns = { code = "text" }\n'
	)
	assert run("load", "-c", str(config)).returncode == 0
	agreed = run("contract", "diff", "--source", "-c", str(config))
	assert (agreed.returncode, agreed.stdout.splitlines()[1]) == (0, "  OK    table:orders"), agreed.stdout + agreed.stderr
	(tmp_path / "data" / "p.parquet").unlink()
	pq.write_table(pa.table({"id": pa.array([2], pa.int64()), "code": pa.array([7], pa.int64()), "qty": ["two"], "fresh": ["a"]}), tmp_path / "data" / "q.parquet")
	assert run("contract", "diff", "-c", str(config)).returncode == 0
	done = run("contract", "diff", "--source", "--json", "-c", str(config))
	findings = json.loads(done.stdout)["resources"][0]["findings"]
	assert (done.returncode, [(finding["kind"], finding["column"], finding.get("source_type"), finding["physical_type"]) for finding in findings]) == (1, [
		("source_type_mismatch", "qty", "text", "int"),
		("source_new_column", "fresh", "text", None),
		("source_missing_column", "gone", None, "bool"),
	]), done.stdout + done.stderr
	config.write_text(config.read_text().replace('description = "Orders."\n', 'description = "Orders."\ncontract = "enforced"\n'))
	enforced = run("contract", "diff", "--source", "--json", "-c", str(config))
	assert [finding["kind"] for finding in json.loads(enforced.stdout)["resources"][0]["findings"] if finding["kind"].startswith("source_")] == [], enforced.stdout + enforced.stderr


def test_given_a_loaded_table_when_generating_its_contract_then_write_declares_the_live_columns_and_overwrite_retypes(tmp_path: Path) -> None:
	import pyarrow as pa
	import pyarrow.parquet as pq

	(tmp_path / "data").mkdir()
	pq.write_table(pa.table({"id": pa.array([1], pa.int64()), "status": ["open"], "amount": [1.5]}), tmp_path / "data" / "p.parquet")
	config = tmp_path / "sqlload.toml"
	config.write_text(
		f'[connections.files]\nkind = "filesystem"\nroot = "{tmp_path}"\n\n[connections.wh]\nkind = "duckdb"\ndatabase = "{tmp_path / "wh.duckdb"}"\n\n'
		'[targets.dev]\nconnection = "wh"\nloader_schema = "RAW"\n\n[sources.orders]\nwrite_strategy = "replace"\nkind = "files"\nconnection = "files"\npath = "data/"\ndescription = "Orders."\n'
	)
	assert run("load", "-c", str(config)).returncode == 0
	config.write_text(config.read_text() + 'contract = "enforced"\n')
	before = config.read_text()
	proposed = run("contract", "generate", "-c", str(config))
	assert proposed.returncode == 1, proposed.stdout + proposed.stderr
	assert proposed.stdout.splitlines()[5:] == [
		"        adopt: columns of [sources.orders.tables.orders]",
		'          + id = "int"',
		'          + status = "text"',
		'          + amount = "float"',
		"",
		"Run contract generate --write to update sqlload.toml.",
		"",
		"3 contract difference(s)",
	], proposed.stdout
	assert config.read_text() == before
	written = run("contract", "generate", "--write", "-c", str(config))
	assert (written.returncode, "Updated repository declarations:\n  sqlload.toml" in written.stdout) == (0, True), written.stdout + written.stderr
	assert config.read_text() == before + '\n[sources.orders.tables.orders.columns]\nid = "int"\nstatus = "text"\namount = "float"\n'
	assert run("contract", "diff", "-c", str(config)).returncode == 0
	config.write_text(config.read_text().replace('status = "text"', 'status = "int"'))
	kept = run("contract", "generate", "--write", "-c", str(config))
	assert (kept.returncode, "type_mismatch: column 'status' is declared int but physical type is text" in kept.stdout) == (1, True), kept.stdout + kept.stderr
	retyped = run("contract", "generate", "--write", "--overwrite", "-c", str(config))
	assert (retyped.returncode, 'status = "text"' in config.read_text()) == (0, True), retyped.stdout + retyped.stderr
	assert run("contract", "generate", "--overwrite", "-c", str(config)).returncode == 2


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
	source = '[sources.orders]\nwrite_strategy = "replace"\nkind = "files"\nconnection = "files"\npath = "data/"\ndescription = "Orders."\ndestination = "{}"\n'
	config.write_text(targets + source.format("prod"))
	assert run("load", "-c", str(config)).returncode == 0
	config.write_text(targets + source.format("dev"))
	assert "relation_missing" in run("contract", "diff", "-c", str(config)).stdout
	from_prod = run("contract", "diff", "--from", "prod", "--json", "-c", str(config))
	assert (from_prod.returncode, json.loads(from_prod.stdout)["resources"][0]["relation"]) == (0, "STAGE.orders"), from_prod.stdout + from_prod.stderr
	unknown = run("contract", "diff", "--from", "staging", "-c", str(config))
	assert (unknown.returncode, "--from staging is not a target in sqlload.toml (it has: dev, lake, prod)" in unknown.stderr) == (1, True), unknown.stdout + unknown.stderr
	from_lake = run("contract", "diff", "--from", "lake", "--json", "-c", str(config))
	assert (from_lake.returncode, [finding["kind"] for finding in json.loads(from_lake.stdout)["resources"][0]["findings"]]) == (1, ["relation_missing"]), from_lake.stdout + from_lake.stderr


@pytest.mark.parametrize(
	("column", "message"),
	[
		("_sqlload_source_file", "_sqlload_source_file must be loaded: append uses it to skip files already loaded"),
		("_sqlload_source_version", "_sqlload_source_version must be loaded: append uses it to tell a file rewritten in place, or a new file under a loaded name, from one already loaded; stop excluding it"),
	],
	ids=["source-file", "source-version"],
)
def test_given_an_append_files_table_excluding_a_column_append_needs_when_planning_then_plan_refuses_it(tmp_path: Path, column: str, message: str) -> None:
	import pyarrow as pa
	import pyarrow.parquet as pq

	(tmp_path / "data").mkdir()
	pq.write_table(pa.table({"id": pa.array([1], pa.int64())}), tmp_path / "data" / "p.parquet")
	config = tmp_path / "sqlload.toml"
	config.write_text(
		f'[connections.files]\nkind = "filesystem"\nroot = "{tmp_path}"\n\n[connections.wh]\nkind = "duckdb"\ndatabase = "{tmp_path / "wh.duckdb"}"\n\n'
		'[targets.dev]\nconnection = "wh"\nloader_schema = "RAW"\n\n[sources.orders]\nwrite_strategy = "replace"\nkind = "files"\nconnection = "files"\npath = "data/"\ndescription = "Orders."\n'
		f'\n[sources.orders.tables.orders]\nwrite_strategy = "append"\nexclude_columns = ["{column}"]\n'
	)
	planned = run("plan", "-c", str(config))
	assert planned.returncode == 1, planned.stdout + planned.stderr
	assert message in planned.stdout + planned.stderr, planned.stdout + planned.stderr


def test_given_parquet_in_a_folder_when_loading_into_duckdb_then_duckdb_reads_the_files_itself_with_the_same_rows(tmp_path: Path) -> None:
	import duckdb
	import pyarrow as pa
	import pyarrow.parquet as pq

	(tmp_path / "data" / "day=2026-01-01").mkdir(parents=True)
	(tmp_path / "data" / "day=2026-01-02").mkdir()
	pq.write_table(pa.table({"id": pa.array([1, 2], pa.int64()), "status": ["open", None]}), tmp_path / "data" / "day=2026-01-01" / "a.parquet")
	pq.write_table(pa.table({"id": pa.array([3], pa.int32()), "amount": [2.5]}), tmp_path / "data" / "day=2026-01-02" / "b.parquet")
	config = tmp_path / "sqlload.toml"
	config.write_text(
		f'[connections.files]\nkind = "filesystem"\nroot = "{tmp_path}"\n\n[connections.wh]\nkind = "duckdb"\ndatabase = "{tmp_path / "wh.duckdb"}"\n\n'
		'[targets.dev]\nconnection = "wh"\nloader_schema = "RAW"\n\n[sources.orders]\nwrite_strategy = "replace"\nkind = "files"\nconnection = "files"\npath = "data/"\n'
		'description = "Orders."\npartition_columns = { day = "date" }\n'
	)
	assert f"DuckDB reads directly from {tmp_path}/data/" in run("plan", "-c", str(config)).stdout
	loaded = run("load", "-c", str(config))
	assert loaded.returncode == 0, loaded.stdout + loaded.stderr
	with duckdb.connect(str(tmp_path / "wh.duckdb"), read_only=True) as connection:
		rows = connection.execute(
			'SELECT id, status, amount, day::VARCHAR, _sqlload_source_file, _sqlload_source_row FROM "RAW"."orders" ORDER BY id'
		).fetchall()
	assert rows == [
		(1, "open", None, "2026-01-01", f"{tmp_path}/data/day=2026-01-01/a.parquet", 1),
		(2, None, None, "2026-01-01", f"{tmp_path}/data/day=2026-01-01/a.parquet", 2),
		(3, None, 2.5, "2026-01-02", f"{tmp_path}/data/day=2026-01-02/b.parquet", 1),
	]


def test_given_a_column_pinned_to_another_type_when_loading_parquet_into_duckdb_then_sqlload_reads_the_files_and_converts_it(tmp_path: Path) -> None:
	import duckdb
	import pyarrow as pa
	import pyarrow.parquet as pq

	(tmp_path / "data").mkdir()
	pq.write_table(pa.table({"id": pa.array([1], pa.int64()), "score": [2.5]}), tmp_path / "data" / "a.parquet")
	config = tmp_path / "sqlload.toml"
	config.write_text(
		f'[connections.files]\nkind = "filesystem"\nroot = "{tmp_path}"\n\n[connections.wh]\nkind = "duckdb"\ndatabase = "{tmp_path / "wh.duckdb"}"\n\n'
		'[targets.dev]\nconnection = "wh"\nloader_schema = "RAW"\n\n[sources.orders]\nwrite_strategy = "replace"\nkind = "files"\nconnection = "files"\npath = "data/"\n'
		'description = "Orders."\n\n[sources.orders.tables.orders]\ncolumns = { score = "text" }\n'
	)
	planned = run("plan", "-c", str(config)).stdout
	assert "DuckDB reads directly" not in planned and "read parquet into Arrow" in planned, planned
	assert run("load", "-c", str(config)).returncode == 0
	with duckdb.connect(str(tmp_path / "wh.duckdb"), read_only=True) as connection:
		assert connection.execute('SELECT id, score FROM "RAW"."orders"').fetchall() == [(1, "2.5")]


@pytest.mark.parametrize(
	"setting",
	[
		'partition_columns = { day = \'native("HUGEINT")\' }',
		'path_columns = \'day={day:native("HUGEINT")}/{file}\'',
		'constant_columns = { day = { value = "7", type = \'native("HUGEINT")\' } }',
	],
	ids=["partition column", "path column", "constant column"],
)
def test_given_a_native_type_for_a_column_from_paths_or_configuration_when_planning_then_plan_refuses_it(tmp_path: Path, setting: str) -> None:
	import pyarrow as pa
	import pyarrow.parquet as pq

	(tmp_path / "data" / "day=7").mkdir(parents=True)
	pq.write_table(pa.table({"id": pa.array([1], pa.int64())}), tmp_path / "data" / "day=7" / "a.parquet")
	config = tmp_path / "sqlload.toml"
	config.write_text(
		f'[connections.files]\nkind = "filesystem"\nroot = "{tmp_path}"\n\n[connections.wh]\nkind = "duckdb"\ndatabase = "{tmp_path / "wh.duckdb"}"\n\n'
		'[targets.dev]\nconnection = "wh"\nloader_schema = "RAW"\n\n[sources.orders]\nwrite_strategy = "replace"\nkind = "files"\nconnection = "files"\npath = "data/"\n'
		f'description = "Orders."\n{setting}\n'
	)
	planned = run("plan", "-c", str(config))
	assert planned.returncode == 1, planned.stdout + planned.stderr
	assert 'column day: native("HUGEINT") has no Arrow type, so a partition, path or constant column cannot be one; pin a portable type' in planned.stdout + planned.stderr, planned.stdout + planned.stderr


def headerless_project(tmp_path: Path, settings: str) -> Path:
	(tmp_path / "data").mkdir(exist_ok=True)
	config = tmp_path / "sqlload.toml"
	config.write_text(
		f'[connections.files]\nkind = "filesystem"\nroot = "{tmp_path}"\n\n[connections.wh]\nkind = "duckdb"\ndatabase = "{tmp_path / "wh.duckdb"}"\n\n'
		'[targets.dev]\nconnection = "wh"\nloader_schema = "RAW"\n\n[sources.metrics]\nkind = "files"\nconnection = "files"\npath = "data/"\ndescription = "Metrics."\n'
		+ settings
	)
	return config


def test_given_headerless_csv_with_column_names_when_merging_then_types_and_keys_use_those_names(tmp_path: Path) -> None:
	import duckdb
	from decimal import Decimal

	config = headerless_project(
		tmp_path,
		'format = "csv"\ncsv = { header = false }\ncolumn_names = ["metric", "category", "value"]\nwrite_strategy = "merge"\nunique_key = ["metric", "category"]\n'
		'\n[sources.metrics.tables.metrics]\ncolumns = { value = "decimal(12,2)" }\n',
	)
	(tmp_path / "data" / "a.csv").write_text("revenue,east,10.5\ncost,west,4\n")
	first = run("load", "-c", str(config))
	assert first.returncode == 0, first.stdout + first.stderr
	(tmp_path / "data" / "b.csv").write_text("revenue,east,11\n")
	second = run("load", "-c", str(config))
	assert second.returncode == 0, second.stdout + second.stderr
	with duckdb.connect(str(tmp_path / "wh.duckdb"), read_only=True) as connection:
		rows = connection.execute('SELECT metric, category, value, typeof(value) FROM "RAW"."metrics" ORDER BY metric').fetchall()
	assert rows == [("cost", "west", Decimal("4.00"), "DECIMAL(12,2)"), ("revenue", "east", Decimal("11.00"), "DECIMAL(12,2)")]


@pytest.mark.parametrize(
	("settings", "files", "expected_text"),
	[
		(
			'format = "csv"\ncolumn_names = ["metric", "value"]\nwrite_strategy = "replace"\n',
			{"a.csv": "metric,value\nrevenue,1\n"},
			'column_names names the columns of data without a header row; set csv = { header = false }',
		),
		(
			'format = "csv"\ncsv = { header = false }\ncolumn_names = ["metric", "Metric"]\nwrite_strategy = "replace"\n',
			{"a.csv": "revenue,1\n"},
			"column_names names Metric twice (ignoring case)",
		),
		(
			'format = "csv"\ncsv = { header = false }\ncolumn_names = ["metric", "value"]\nwrite_strategy = "replace"\n',
			{"a.csv": "revenue,east,1\n"},
			"line 1: the file has 3 columns, but column_names names 2; name each column",
		),
		(
			'format = "xlsx"\nxlsx = { range = "Prices!A3:C", skip_rows = 2 }\nwrite_strategy = "replace"\n',
			{},
			"xlsx.range names the sheet and the first row itself; drop xlsx.sheet and xlsx.skip_rows",
		),
		(
			'format = "parquet"\ncolumn_names = ["metric"]\nwrite_strategy = "replace"\n',
			{},
			'column_names is for columns of data without a header row: kind = "files" with format = "csv" or "xlsx" (not shape = "record"), or kind = "google_sheets"',
		),
	],
	ids=["with-a-header", "repeated-ignoring-case", "count-differs", "range-with-skip-rows", "parquet"],
)
def test_given_column_names_or_an_xlsx_range_that_do_not_fit_when_planning_then_plan_fails_saying_why(
	tmp_path: Path, settings: str, files: dict[str, str], expected_text: str
) -> None:
	config = headerless_project(tmp_path, settings)
	for name, text in files.items():
		(tmp_path / "data" / name).write_text(text)
	done = run("plan", "-c", str(config))
	assert done.returncode == 1, done.stdout + done.stderr
	assert expected_text in done.stdout + done.stderr, done.stdout + done.stderr


LIBRARY = "/usr/lib/x86_64-linux-gnu/libstdc++.so.6"


@pytest.mark.parametrize(
	("environ", "platform", "found", "expected"),
	[
		pytest.param({}, "linux", LIBRARY, {_cli.PRELOADED: "1", "LD_PRELOAD": LIBRARY}, id="linux starts again with libstdc++ preloaded"),
		pytest.param({"LD_PRELOAD": "/opt/a.so"}, "linux", LIBRARY, {_cli.PRELOADED: "1", "LD_PRELOAD": f"{LIBRARY} /opt/a.so"}, id="a preload already set is kept after it"),
		pytest.param({_cli.PRELOADED: "1"}, "linux", LIBRARY, None, id="a process that started again never starts again"),
		pytest.param({"LD_PRELOAD": LIBRARY}, "linux", LIBRARY, None, id="libstdc++ preloaded already"),
		pytest.param({_cli.NO_PRELOAD: "1"}, "linux", LIBRARY, None, id="SQLLOAD_NO_PRELOAD turns it off"),
		pytest.param({}, "linux", None, None, id="no libstdc++ installed: runs as it is"),
		pytest.param({}, "darwin", LIBRARY, None, id="not on Linux"),
	],
)
def test_given_the_environment_when_the_console_script_starts_then_it_starts_again_with_libstdcxx_preloaded_at_most_once(
	environ: dict[str, str], platform: str, found: str | None, expected: dict[str, str] | None
) -> None:
	assert _cli.preloaded_environment(environ, platform, lambda: found) == (None if expected is None else {**environ, **expected})


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="the preload is Linux-only")
def test_given_the_console_script_on_linux_when_run_then_it_starts_again_once_with_libstdcxx_preloaded() -> None:
	code = "import os, sys; print(repr(os.environ.get('LD_PRELOAD')), flush=True); sys.argv[1:] = ['--version']; from sqlload._cli import main; main()"
	env = {key: value for key, value in os.environ.items() if key not in ("LD_PRELOAD", _cli.PRELOADED, _cli.NO_PRELOAD)}
	done = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env)
	assert done.returncode == 0, done.stderr
	first, second, version = done.stdout.splitlines()
	assert first == "None" and "libstdc++.so.6" in second and version.startswith("sqlload "), done.stdout


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="the preload is Linux-only")
def test_given_a_script_read_from_stdin_when_it_runs_the_console_script_then_sqlload_runs_without_starting_again() -> None:
	script = "import sys; print('run', flush=True); sys.argv[1:] = ['--version']; from sqlload._cli import main; main()"
	env = {key: value for key, value in os.environ.items() if key not in ("LD_PRELOAD", _cli.PRELOADED, _cli.NO_PRELOAD)}
	done = subprocess.run([sys.executable, "-"], input=script, capture_output=True, text=True, env=env)
	assert done.returncode == 0, done.stderr
	assert done.stdout.splitlines()[0] == "run" and done.stdout.splitlines()[-1].startswith("sqlload "), done.stdout
