"""MotherDuck targets through the bridge, offline: `md:` databases opened by a stand-in for MotherDuck that keeps each one in a local DuckDB file, beside the same tables loaded into DuckDB itself."""

import json
import os
import subprocess
import sys
import textwrap
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pytest

pytestmark = pytest.mark.skipif(os.name == "nt", reason="the stand-in Python is a shell script")

PROJECT = """\
python_paths = ["sources"]

[connections.wh]
kind = "duckdb"
database = "wh.duckdb"

[connections.md]
kind = "motherduck"
database = "shop"
token = "${ENV:SHOP_MD_TOKEN}"

[targets.local]
connection = "wh"
loader_schema = "raw"

[targets.cloud]
connection = "md"
loader_schema = "raw"
"""

#: The token the project signs in with: `&`, `=` and `#` must reach MotherDuck whole.
TOKEN = "tok&en=#1"

#: Stands in for MotherDuck in the bridge's Python: `md:<name>?motherduck_token=...` opens `<name>.duckdb` in MD_FOLDER
#: after checking the token, with another database attached as MotherDuck attaches the account's others. MD_REFUSE makes
#: it refuse as MotherDuck's extension does on a DuckDB version it doesn't support; MD_DIE_ON ends the bridge when a
#: statement holds that text; MD_LOG records every statement.
STAND_IN = r'''
import os
import sys
import urllib.parse

import duckdb

_connect = duckdb.connect
FOLDER = os.environ["MD_FOLDER"]


def log(sql):
    if os.environ.get("MD_LOG"):
        with open(os.environ["MD_LOG"], "a") as out:
            out.write(" ".join(sql.split()) + "\n")


class Cursor:
    def __init__(self, inner):
        self.inner = inner

    def execute(self, sql, params=None):
        log(sql)
        if os.environ.get("MD_DIE_ON") and os.environ["MD_DIE_ON"] in sql:
            sys.stderr.write(f"the stand-in for MotherDuck ended the bridge, signed in as {os.environ['MD_EXPECTED_TOKEN']}\n")
            sys.stderr.flush()
            os._exit(3)
        return self.inner.execute(sql, params)

    def __getattr__(self, name):
        return getattr(self.inner, name)


class Database:
    def __init__(self, inner):
        self.inner = inner

    def cursor(self):
        return Cursor(self.inner.cursor())


def connect(database=":memory:", *args, config=None, **kwargs):
    if not str(database).startswith("md:"):
        return _connect(database, *args, config=config, **kwargs)
    log(f"python: {sys.executable}")
    name, _, query = database[3:].partition("?")
    token = dict(urllib.parse.parse_qsl(query)).get("motherduck_token")
    if token != os.environ["MD_EXPECTED_TOKEN"]:
        raise duckdb.InvalidInputException("Invalid Input Error: the token was not the one expected")
    if os.environ.get("MD_REFUSE"):
        raise duckdb.InvalidInputException(
            'Invalid Input Error: Initialization function "motherduck_duckdb_cpp_init" from file "motherduck.duckdb_extension" '
            'threw an exception: "\nYour DuckDB version (v2.0.0) is not yet supported by MotherDuck. The latest supported '
            f'version is v1.5.6. Please downgrade to use MotherDuck. {database}\n"'
        )
    inner = _connect(os.path.join(FOLDER, f"{name}.duckdb"), config=config or {})
    inner.execute(f"ATTACH '{os.path.join(FOLDER, 'other.duckdb')}' AS other")
    inner.execute("CREATE SCHEMA IF NOT EXISTS other.raw; CREATE TABLE IF NOT EXISTS other.raw.cloud__cloud_rows (stray INTEGER)")
    return Database(inner)


duckdb.connect = connect
'''

#: Each table yields the rows in data/<table>.json; a row whose `raise` is set raises it after the rows before it.
MODULE = '''
import json
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import sqlload as sl

HERE = Path(__file__).resolve().parent.parent


def rows(ctx):
    # local_rows and cloud_rows share data/rows.json: table names are unique across a project.
    for row in json.loads((HERE / "data" / f"{ctx.table.removeprefix('local_').removeprefix('cloud_')}.json").read_text()):
        if "raise" in row:
            raise RuntimeError(row["raise"])
        yield row


def typed(ctx):
    yield {
        "flag": True, "small": 7, "big": 9007199254740993, "amount": Decimal("12.50"), "ratio": 0.25,
        "label": "café ☕", "code": "ab", "day": date(2026, 11, 12), "at_time": time(13, 45, 30, 123456),
        "at_utc": datetime(2026, 11, 12, 9, 30, 0, 654321, tzinfo=timezone.utc), "at_local": datetime(2026, 11, 12, 9, 30, 1),
        "doc": {"k": [1, "two"]}, "uid": "0f8fad5b-d9cb-469f-a165-70867728950e", "tags": ["a", "b"],
        "empty": None,
    }


TYPED = dict(
    description="Every portable type.",
    columns={
        "flag": "bool", "small": "smallint", "big": "int", "amount": "decimal(10,2)", "ratio": "float", "label": "text",
        "code": "varchar(4)", "day": "date", "at_time": "time", "at_utc": "timestamp_tz", "at_local": "timestamp_ntz",
        "doc": "json", "uid": "uuid", "blob": "binary", "tags": "array(text)", "empty": "text",
    },
)

local = sl.source("local", write_strategy="replace", destination="local")
cloud = sl.source("cloud", write_strategy="replace", destination="cloud")

{tables}
'''


def make_project(tmp_path: Path, tables: str) -> Path:
	for folder in ("sources", "data", "md", "bin", "shim", "home"):
		(tmp_path / folder).mkdir()
	(tmp_path / "sqlload.toml").write_text(PROJECT)
	(tmp_path / "sources" / "shop.py").write_text(MODULE.replace("{tables}", textwrap.dedent(tables)))
	(tmp_path / "shim" / "sitecustomize.py").write_text(STAND_IN)
	python = tmp_path / "bin" / "python"
	python.write_text(f'#!/bin/sh\nPYTHONPATH="{tmp_path / "shim"}" exec "{sys.executable}" "$@"\n')
	python.chmod(0o755)
	return tmp_path


def give(project: Path, table: str, rows: Any) -> None:
	(project / "data" / f"{table}.json").write_text(json.dumps(rows))


def sqlload(project: Path, *args: str, **env: str) -> subprocess.CompletedProcess[str]:
	home = project / "home"
	environment = {
		**os.environ,
		"HOME": str(home),
		"XDG_CONFIG_HOME": str(home / ".config"),
		"XDG_CACHE_HOME": str(home / ".cache"),
		"SHOP_MD_TOKEN": TOKEN,
		"MD_EXPECTED_TOKEN": TOKEN,
		"MD_FOLDER": str(project / "md"),
		"SQLLOAD_MOTHERDUCK_PYTHON": str(project / "bin" / "python"),
	}
	for name in ("MOTHERDUCK_TOKEN", "motherduck_token", "PYTHONPATH"):
		environment.pop(name, None)
	environment.update(env)
	return subprocess.run(
		[sys.executable, "-c", "from sqlload._cli import main; main()", *args], cwd=project, env=environment, capture_output=True, text=True
	)


def load(project: Path, *args: str, **env: str) -> subprocess.CompletedProcess[str]:
	done = sqlload(project, "load", *args, **env)
	assert done.returncode == 0, done.stdout + done.stderr
	return done


def query(path: Path, sql: str) -> list[tuple[Any, ...]]:
	with duckdb.connect(str(path), read_only=True) as connection:
		return connection.execute(sql).fetchall()


def cloud(project: Path, sql: str) -> list[tuple[Any, ...]]:
	return query(project / "md" / "shop.duckdb", sql)


def local(project: Path, sql: str) -> list[tuple[Any, ...]]:
	return query(project / "wh.duckdb", sql)


@pytest.mark.parametrize(
	("settings", "first", "second", "expected"),
	[
		("write_strategy='append'", [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}], [{"id": 3, "v": "c"}], [(1, "a"), (2, "b"), (3, "c")]),
		(
			"write_strategy='merge', unique_key='id'",
			[{"id": 1, "v": "a"}, {"id": 2, "v": "b"}],
			[{"id": 2, "v": "B"}, {"id": 3, "v": "c"}],
			[(1, "a"), (2, "B"), (3, "c")],
		),
		("write_strategy='replace'", [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}], [{"id": 3, "v": "c"}], [(3, "c")]),
		(
			"write_strategy='delete_insert', cursor_column='day'",
			[{"id": 1, "v": "a", "day": "2026-01-01T00:00:00Z"}, {"id": 2, "v": "b", "day": "2026-01-02T00:00:00Z"}],
			[
				{"id": 1, "v": "stale", "day": "2026-01-01T00:00:00Z"},
				{"id": 2, "v": "B", "day": "2026-01-02T00:00:00Z"},
				{"id": 3, "v": "c", "day": "2026-01-03T00:00:00Z"},
			],
			[(1, "a"), (2, "B"), (3, "c")],
		),
	],
	ids=["append", "merge", "replace", "delete_insert"],
)
def test_given_a_table_when_loaded_twice_into_motherduck_then_its_strategy_applies_as_on_duckdb(
	tmp_path: Path, settings: str, first: list[dict[str, Any]], second: list[dict[str, Any]], expected: list[tuple[Any, ...]]
) -> None:
	project = make_project(tmp_path, f"local.table(description='Rows.', name='local_rows', {settings})(rows)\ncloud.table(description='Rows.', name='cloud_rows', {settings})(rows)\n")
	give(project, "rows", first)
	load(project)
	give(project, "rows", second)
	load(project)
	assert cloud(project, "SELECT id, v FROM raw.cloud__cloud_rows ORDER BY id") == expected
	assert local(project, "SELECT id, v FROM raw.local__local_rows ORDER BY id") == expected
	assert cloud(project, "SELECT count(*) FROM information_schema.tables WHERE table_name LIKE '%sqlload_staging%'") == [(0,)]
	assert query(project / "md" / "other.duckdb", "SELECT column_name FROM information_schema.columns WHERE table_name = 'cloud__cloud_rows'") == [("stray",)]


def test_given_every_portable_type_when_loaded_then_motherduck_holds_what_duckdb_holds(tmp_path: Path) -> None:
	project = make_project(tmp_path, "local.table(name='local_typed', **TYPED)(typed)\ncloud.table(name='cloud_typed', **TYPED)(typed)\n")
	load(project)
	columns = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{}' ORDER BY ordinal_position"
	cloud_columns = cloud(project, columns.format("cloud__cloud_typed"))
	assert cloud_columns == local(project, columns.format("local__local_typed"))
	assert dict(cloud_columns)["at_utc"] == "TIMESTAMP WITH TIME ZONE"
	values = "SET TimeZone = 'UTC'; SELECT CAST(COLUMNS(* EXCLUDE (_sqlload_load_id, _sqlload_loaded_at)) AS VARCHAR) FROM raw.{}"
	loaded = cloud(project, values.format("cloud__cloud_typed"))
	assert loaded == local(project, values.format("local__local_typed"))
	assert dict(zip([name for name, _ in cloud_columns], loaded[0], strict=False)) == {
		"flag": "true",
		"small": "7",
		"big": "9007199254740993",
		"amount": "12.50",
		"ratio": "0.25",
		"label": "café ☕",
		"code": "ab",
		"day": "2026-11-12",
		"at_time": "13:45:30.123456",
		"at_utc": "2026-11-12 09:30:00.654321+00",
		"at_local": "2026-11-12 09:30:01",
		"doc": '{"k":[1,"two"]}',
		"uid": "0f8fad5b-d9cb-469f-a165-70867728950e",
		"blob": None,
		"tags": "[a, b]",
		"empty": None,
	}


def test_given_an_incremental_table_when_loaded_again_and_rerun_then_only_new_rows_arrive(tmp_path: Path) -> None:
	project = make_project(tmp_path, "cloud.table(description='Rows.', name='rows', write_strategy='append', cursor_column='at')(rows)\n")
	give(project, "rows", [{"id": 1, "at": "2026-01-01T10:00:00Z"}, {"id": 2, "at": "2026-01-02T10:00:00Z"}])
	load(project)
	give(project, "rows", [{"id": 2, "at": "2026-01-02T10:00:00Z"}, {"id": 3, "at": "2026-01-03T10:00:00Z", "note": "new column"}])
	load(project)
	load(project)
	assert cloud(project, "SELECT id, note FROM raw.cloud__rows ORDER BY id") == [(1, None), (2, None), (3, "new column")]


def test_given_the_bridge_ends_mid_load_when_loading_then_the_table_fails_keeps_its_rows_and_quotes_it_without_the_token(tmp_path: Path) -> None:
	project = make_project(tmp_path, "cloud.table(description='Rows.', name='rows', write_strategy='merge', unique_key='id')(rows)\n")
	give(project, "rows", [{"id": 1, "v": "a"}])
	load(project)
	give(project, "rows", [{"id": 1, "v": "A"}, {"id": 2, "v": "b"}])
	done = sqlload(project, "load", MD_DIE_ON="read_parquet")
	assert done.returncode == 1, done.stdout + done.stderr
	assert "the MotherDuck bridge on" in done.stdout + done.stderr
	assert "the stand-in for MotherDuck ended the bridge, signed in as <redacted>" in done.stdout + done.stderr
	assert TOKEN not in done.stdout + done.stderr
	assert cloud(project, "SELECT id, v FROM raw.cloud__rows ORDER BY id") == [(1, "a")]
	load(project)
	assert cloud(project, "SELECT id, v FROM raw.cloud__rows ORDER BY id") == [(1, "A"), (2, "b")]


def test_given_a_value_motherduck_rejects_when_loading_then_its_error_is_relayed_and_nothing_commits(tmp_path: Path) -> None:
	project = make_project(tmp_path, "cloud.table(description='Rows.', name='rows', write_strategy='append', columns={'doc': 'json'})(rows)\n")
	give(project, "rows", [{"id": 1, "doc": "{}"}])
	load(project)
	give(project, "rows", [{"id": 2, "doc": "{}"}, {"id": 3, "doc": "{not json"}])
	done = sqlload(project, "load")
	assert done.returncode == 1, done.stdout + done.stderr
	assert "MotherDuck: Conversion Error" in done.stdout + done.stderr
	assert cloud(project, "SELECT id FROM raw.cloud__rows ORDER BY id") == [(1,)]


@pytest.mark.parametrize(
	("env", "expected"),
	[
		(
			{"SQLLOAD_MOTHERDUCK_PYTHON": "/no/such/python"},
			"MotherDuck connects through Python's duckdb package (duckdb 1.4.1 to 1.5.6), and /no/such/python cannot run",
		),
		(
			{"MD_REFUSE": "1"},
			"MotherDuck doesn't accept the version of Python's duckdb package that SQLLoad connects through: install duckdb 1.4.1 to 1.5.6 there (pip install 'sqlload[motherduck]'), or set SQLLOAD_MOTHERDUCK_PYTHON to a Python that has it. MotherDuck says: Your DuckDB version (v2.0.0) is not yet supported by MotherDuck.",
		),
		({"MD_EXPECTED_TOKEN": "another"}, "MotherDuck: cannot open md:shop: Invalid Input Error: the token was not the one expected"),
	],
	ids=["no-python", "refused-version", "refused-token"],
)
def test_given_the_bridge_cannot_open_motherduck_when_debugging_then_it_says_why_without_the_token(
	tmp_path: Path, env: dict[str, str], expected: str
) -> None:
	project = make_project(tmp_path, "")
	done = sqlload(project, "debug", **env)
	output = done.stdout + done.stderr
	assert done.returncode == 1, output
	assert expected in output
	assert "tok" not in output.replace("token", "")


def test_given_a_python_without_duckdb_when_debugging_then_it_says_to_install_the_extra(tmp_path: Path) -> None:
	project = make_project(tmp_path, "")
	(project / "shim" / "sitecustomize.py").write_text("import sys\nsys.modules['duckdb'] = None\n")
	done = sqlload(project, "debug")
	assert done.returncode == 1, done.stdout + done.stderr
	assert "lacks (" in done.stdout + done.stderr
	assert "install sqlload[motherduck] with pip or uv" in done.stdout + done.stderr


def test_given_no_python_is_named_when_debugging_then_motherduck_runs_on_this_packages_python(tmp_path: Path) -> None:
	project = make_project(tmp_path, "")
	log = project / "statements.log"
	done = sqlload(project, "debug", SQLLOAD_MOTHERDUCK_PYTHON="", PYTHONPATH=str(project / "shim"), MD_LOG=str(log))
	assert done.returncode == 0, done.stdout + done.stderr
	assert "MotherDuck shop, DuckDB v" in done.stdout
	assert f"python: {sys.executable}" in log.read_text().splitlines()
