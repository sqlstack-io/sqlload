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
import pyarrow
import pyarrow.parquet as pyarrow_parquet
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
#: statement holds that text; MD_LOG records every statement. MD_CLIENT_VERSION is the duckdb version the bridge reports,
#: and MD_SERVER_VERSION what `md_version()` answers (without it, the function doesn't exist). MD_READ_ONLY opens each
#: database read-only, so a source's bridges (one per table) can hold it at once as MotherDuck allows.
STAND_IN = r'''
import os
import sys
import urllib.parse

import duckdb

_connect = duckdb.connect
FOLDER = os.environ["MD_FOLDER"]
duckdb.__version__ = os.environ.get("MD_CLIENT_VERSION", duckdb.__version__)


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
    read_only = {"access_mode": "READ_ONLY"} if os.environ.get("MD_READ_ONLY") else {}
    inner = _connect(os.path.join(FOLDER, f"{name}.duckdb"), config={**(config or {}), **read_only})
    if read_only:
        return Database(inner)
    inner.execute(f"ATTACH '{os.path.join(FOLDER, 'other.duckdb')}' AS other")
    inner.execute("CREATE SCHEMA IF NOT EXISTS other.raw; CREATE TABLE IF NOT EXISTS other.raw.cloud__cloud_rows (stray INTEGER)")
    if os.environ.get("MD_SERVER_VERSION"):
        inner.execute(f"CREATE OR REPLACE MACRO md_version() AS '{os.environ['MD_SERVER_VERSION']}'")
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
			"write_strategy='delete_insert', cursor_column='day', on_cursor_out_of_range='skip'",
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
	project = make_project(tmp_path, "cloud.table(description='Rows.', name='rows', write_strategy='append', cursor_column='at', on_cursor_out_of_range='skip')(rows)\n")
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


#: Tables that note in the stand-in's statement log (`-- read ...`) when they have read their rows, so the log shows what reached
#: MotherDuck while they were read: `windowed` reads two parts per cursor window, `failing` one part before it fails.
MARKED = """
import os
import time

import pyarrow as pa


def mark(text):
    with open(os.environ["MD_LOG"], "a") as out:
        out.write(f"-- {text}\\n")


def windowed(ctx):
    start = int(ctx.cursor_value)
    for part in range(2):
        yield pa.table({"n": [start] * 100000, "part": [part] * 100000})
        time.sleep(0.2)
    mark(f"read window {start}")


def failing(ctx):
    yield pa.table({"n": list(range(100000))})
    time.sleep(0.2)
    mark("read part 0")
    raise RuntimeError("the API went away")


cloud.table(description="Rows.", name="windowed", write_strategy="append", cursor_column="n", batch_size=1{send})(windowed)
cloud.table(description="Rows.", name="failing", write_strategy="replace"{send})(failing)
"""


def inserts_between_reads(log: Path) -> list[int]:
	"""How many INSERTs reached MotherDuck before the first `-- read` note, then after each."""
	counts = [0]
	for line in log.read_text().splitlines():
		if line.startswith("-- read"):
			counts.append(0)
		elif line.startswith("INSERT"):
			counts[-1] += 1
	return counts


@pytest.mark.parametrize(
	("send", "expected_before_first_read", "expected_after_each_read"),
	[("", 0, [2, 2, 2]), (", send='per_batch'", None, None)],
	ids=["at_commit-by-default", "per_batch"],
)
def test_given_send_when_a_table_loads_in_windows_then_at_commit_sends_each_window_in_one_burst_after_its_read(
	tmp_path: Path, send: str, expected_before_first_read: int | None, expected_after_each_read: list[int] | None
) -> None:
	project = make_project(tmp_path, MARKED.replace("{send}", send))
	log = tmp_path / "statements.log"
	load(project, "windowed", "--start-cursor", "0", "--end-cursor", "3", MD_LOG=str(log))
	counts = inserts_between_reads(log)
	if expected_before_first_read is None:
		assert counts[0] >= 1, log.read_text()
	else:
		assert (counts[0], counts[1:]) == (expected_before_first_read, expected_after_each_read), log.read_text()
	assert cloud(project, "SELECT n, part, count(*) FROM raw.cloud__windowed GROUP BY ALL ORDER BY ALL") == [
		(n, part, 100000) for n in range(3) for part in range(2)
	]


def test_given_send_at_commit_when_a_read_fails_part_way_then_nothing_reaches_motherduck_and_no_held_file_stays(tmp_path: Path) -> None:
	project = make_project(tmp_path, MARKED.replace("{send}", ""))
	(project / "sqlload.toml").write_text(
		(project / "sqlload.toml").read_text().replace('[targets.cloud]\nconnection = "md"\n', '[targets.cloud]\nconnection = "md"\nstaging_dir = "held"\n')
	)
	(project / "held").mkdir()
	log = tmp_path / "statements.log"
	done = sqlload(project, "load", "failing", MD_LOG=str(log))
	assert done.returncode == 1, done.stdout + done.stderr
	assert "the API went away" in done.stdout
	assert inserts_between_reads(log) == [0, 0], log.read_text()
	assert list((project / "held").iterdir()) == []


def test_given_send_when_planning_then_each_table_says_how_its_rows_are_sent(tmp_path: Path) -> None:
	project = make_project(tmp_path, MARKED.replace("{send}", ""))
	(project / "drop").mkdir()
	pyarrow_parquet.write_table(pyarrow.table({"id": [1, 2]}), project / "drop" / "a.parquet")
	(project / "sqlload.toml").write_text(
		(project / "sqlload.toml").read_text().replace('[targets.local]\nconnection = "wh"\n', '[targets.local]\nconnection = "wh"\nsend = "at_commit"\n')
		+ '\n[connections.files]\nkind = "filesystem"\nroot = "."\n\n[sources.drop]\nkind = "files"\ndescription = "Dropped files."\nconnection = "files"\npath = "drop/"\nformat = "parquet"\nwrite_strategy = "append"\ndestination = "local"\n'
	)
	done = sqlload(project, "plan", "-v", "failing", "drop")
	assert done.returncode == 0, done.stdout + done.stderr
	assert "send    at commit: staged locally, then one burst" in done.stdout, done.stdout
	assert "send    n/a: DuckDB reads the files itself" in done.stdout, done.stdout


SLOW_VARIANT = "MotherDuck runs DuckDB 1.5, which stores VARIANT unshredded: queries on its fields can be much slower than on JSON until MotherDuck moves to DuckDB 2.0; json_type = \"json\" keeps JSON"


@pytest.mark.parametrize(
	("env", "expected_code", "expected"),
	[
		({"MD_CLIENT_VERSION": "1.5.6", "MD_SERVER_VERSION": "v1.5.6-2026-11-382"}, 0, SLOW_VARIANT),
		({"MD_CLIENT_VERSION": "1.5.6"}, 0, SLOW_VARIANT + " (judged from the bridge's duckdb 1.5.6: MotherDuck reported no server version)"),
		(
			{"MD_CLIENT_VERSION": "1.4.4", "MD_SERVER_VERSION": "v1.5.6-2026-11-382"},
			1,
			"VARIANT columns need Python's duckdb 1.5 or later on the MotherDuck bridge, which runs 1.4.4: install duckdb 1.5 there (pip install 'sqlload[motherduck]'), or set json_type = \"json\" on the target",
		),
		({"MD_CLIENT_VERSION": "1.5.6", "MD_SERVER_VERSION": "v2.0.0-2027-01-1"}, 0, ""),
	],
	ids=["server-1.5", "no-server-version", "client-1.4", "server-2.0"],
)
def test_given_json_type_variant_on_motherduck_when_planning_and_loading_then_the_versions_decide_warnings_and_refusals(
	tmp_path: Path, env: dict[str, str], expected_code: int, expected: str
) -> None:
	project = make_project(tmp_path, "cloud.table(description='Dict rows.', name='cloud_rows')(rows)\n")
	(project / "sqlload.toml").write_text(PROJECT.replace('[targets.cloud]\nconnection = "md"\n', '[targets.cloud]\nconnection = "md"\njson_type = "variant"\n'))
	give(project, "rows", [{"id": 1, "payload": {"user": {"id": 42}}}])
	planned = sqlload(project, "plan", "cloud_rows", **env)
	assert planned.returncode == expected_code and expected in " ".join(planned.stdout.split()), planned.stdout + planned.stderr
	assert ("unshredded" in planned.stdout) == ("unshredded" in expected), planned.stdout
	if expected_code == 0:
		loaded = load(project, "cloud_rows", **env)
		assert ("unshredded" in loaded.stdout + loaded.stderr) == ("unshredded" in expected), loaded.stdout + loaded.stderr
		assert cloud(project, 'SELECT variant_typeof(payload), CAST(payload.user.id AS BIGINT) FROM raw.cloud__cloud_rows') == [("OBJECT(user)", 42)]


#: A project reading the stand-in's `shop` database as a motherduck source into the local DuckDB file.
SOURCE_PROJECT = """\
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

[sources.shop]
kind = "motherduck"
connection = "md"
destination = "local"
description = "The shop's MotherDuck database."
include = ["sales.*"]
exclude = ["sales.tmp_*"]
exclude_columns = ["card"]
write_strategy = "replace"

[sources.shop.tables."sales.orders"]
description = "Orders, merged after their cursor."
write_strategy = "merge"
unique_key = ["id"]
cursor_column = "updated_at"
where = "region = 'EU'"

[sources.shop.tables.big_query]
description = "Orders over 25, by query."
sql = "SELECT id, amount FROM sales.orders WHERE amount > 25 -- big ones"
"""

SHOP = """
CREATE SCHEMA sales;
CREATE TABLE sales.orders (id INTEGER PRIMARY KEY, amount DECIMAL(12,2), updated_at TIMESTAMP, tags VARCHAR[], card VARCHAR, region VARCHAR);
INSERT INTO sales.orders VALUES
    (1, 10.50, '2026-01-01 00:00:00', ['a'], '4111', 'EU'),
    (2, 20.00, '2026-01-02 00:00:00', [], '4222', 'EU'),
    (3, 30.25, '2026-01-03 00:00:00', NULL, '4333', 'US');
CREATE VIEW sales.big_orders AS SELECT id, amount FROM sales.orders WHERE amount > 15;
CREATE TABLE sales.tmp_scratch (x INTEGER);
"""

ORDERS = "SELECT id, CAST(amount AS VARCHAR), CAST(tags AS VARCHAR) FROM raw.shop__orders ORDER BY id"


def test_given_a_motherduck_source_when_loading_again_then_its_tables_views_and_queries_load_through_the_bridge_and_the_cursor_merges_changes(
	tmp_path: Path,
) -> None:
	project = make_project(tmp_path, "")
	(project / "sqlload.toml").write_text(SOURCE_PROJECT)
	with duckdb.connect(str(project / "md" / "shop.duckdb")) as shop:
		shop.execute(SHOP)
	statements = project / "statements.log"
	debug = sqlload(project, "debug", MD_LOG=str(statements), MD_READ_ONLY="1")
	assert debug.returncode == 0 and "shop on MotherDuck, through DuckDB" in debug.stdout, debug.stdout + debug.stderr
	assert TOKEN not in debug.stdout + debug.stderr
	planned = sqlload(project, "plan", MD_READ_ONLY="1")
	assert planned.returncode == 0, planned.stdout + planned.stderr
	assert "tmp_scratch" not in planned.stdout, planned.stdout
	load(project, MD_READ_ONLY="1")
	assert local(project, ORDERS) == [(1, "10.50", "[\"a\"]"), (2, "20.00", "[]")]
	assert local(project, "SELECT count(*) FROM raw.shop__big_orders") == [(2,)]
	assert local(project, "SELECT id FROM raw.shop__big_query ORDER BY id") == [(3,)]
	columns = "SELECT column_name FROM information_schema.columns WHERE table_name = 'shop__orders' AND column_name NOT LIKE '\\_sqlload%' ESCAPE '\\'"
	assert sorted(row[0] for row in local(project, columns)) == ["amount", "id", "region", "tags", "updated_at"]
	with duckdb.connect(str(project / "md" / "shop.duckdb")) as shop:
		shop.execute("UPDATE sales.orders SET amount = 21.00, updated_at = '2026-01-05 00:00:00' WHERE id = 2")
		shop.execute("INSERT INTO sales.orders VALUES (4, 40.00, '2026-01-06 00:00:00', ['b', 'c'], '4444', 'EU')")
	load(project, MD_LOG=str(statements), MD_READ_ONLY="1")
	assert local(project, ORDERS) == [(1, "10.50", "[\"a\"]"), (2, "21.00", "[]"), (4, "40.00", "[\"b\",\"c\"]")]
	reads = [line for line in statements.read_text().splitlines() if line.startswith("SELECT \"id\"") and "sales\".\"orders\"" in line]
	assert reads and all("\"updated_at\" >" in read for read in reads), reads
