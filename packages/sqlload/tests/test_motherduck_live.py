"""MotherDuck targets against a live MotherDuck account, on duckdb 1.5.6. Runs only with SQLLOAD_TEST_MOTHERDUCK_TOKEN set; each test creates
a database named sqlload_test_<random> and drops it afterwards."""

import json
import os
import shutil
import subprocess
import sys
import textwrap
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

TOKEN_VARIABLE = "SQLLOAD_TEST_MOTHERDUCK_TOKEN"

pytestmark = pytest.mark.skipif(not os.environ.get(TOKEN_VARIABLE), reason=f"{TOKEN_VARIABLE} is not set")

PROJECT = """\
python_paths = ["sources"]

[connections.md]
kind = "motherduck"
database = "{database}"
token = "${{ENV:{variable}}}"

[targets.cloud]
connection = "md"
loader_schema = "raw"
"""

MODULE = '''
import json
from pathlib import Path

import sqlload as sl

HERE = Path(__file__).resolve().parent.parent
cloud = sl.source("cloud", write_strategy="replace", destination="cloud")


def rows(ctx):
    yield from json.loads((HERE / "data" / f"{ctx.table}.json").read_text())


cloud.table(description="Appended by cursor.", name="events", write_strategy="append", cursor_column="at")(rows)
cloud.table(description="Merged by id.", name="customers", write_strategy="merge", unique_key="id")(rows)
cloud.table(description="Replaced by cursor window.", name="days", write_strategy="delete_insert", cursor_column="day")(rows)
cloud.table(description="Replaced whole.", name="snapshot", write_strategy="replace")(rows)
cloud.table(
    description="Every portable type.",
    name="typed",
    columns={
        "flag": "bool", "small": "smallint", "big": "int", "amount": "decimal(10,2)", "ratio": "float", "label": "text",
        "day": "date", "at_utc": "timestamp_tz", "at_local": "timestamp_ntz", "doc": "json", "uid": "uuid", "tags": "array(text)",
    },
)(rows)
'''

#: Runs SQL on MotherDuck from the duckdb 1.5.6 environment: the database and statements as JSON on stdin, the last rows as JSON on stdout.
ADMIN = """
import json, sys, urllib.parse
import duckdb
request = json.load(sys.stdin)
connection = duckdb.connect("md:?motherduck_token=" + urllib.parse.quote(request["token"], safe=""))
rows = []
for sql in request["sql"]:
    rows = connection.execute(sql).fetchall()
print(json.dumps(rows, default=str))
"""


@pytest.fixture(scope="module")
def python(tmp_path_factory: pytest.TempPathFactory) -> str:
	"""A Python with duckdb 1.5.6, the newest version MotherDuck accepts."""
	folder = tmp_path_factory.mktemp("duckdb15")
	uv = shutil.which("uv")
	assert uv, "the live MotherDuck tests need uv to build a duckdb 1.5.6 environment"
	subprocess.run([uv, "venv", "--quiet", "--python", "3.12", str(folder / "venv")], check=True)
	executable = folder / "venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
	subprocess.run([uv, "pip", "install", "--quiet", "--python", str(executable), "duckdb==1.5.6"], check=True)
	return str(executable)


def admin(python: str, *sql: str) -> list[list[Any]]:
	request = json.dumps({"token": os.environ[TOKEN_VARIABLE], "sql": list(sql)})
	done = subprocess.run([python, "-I", "-c", ADMIN], input=request, capture_output=True, text=True)
	assert done.returncode == 0, done.stderr.replace(os.environ[TOKEN_VARIABLE], "<redacted>")
	return json.loads(done.stdout)


@pytest.fixture
def database(python: str) -> Iterator[str]:
	name = f"sqlload_test_{uuid.uuid4().hex[:12]}"
	admin(python, f"CREATE DATABASE {name}")
	try:
		yield name
	finally:
		admin(python, f"DROP DATABASE IF EXISTS {name}")


def sqlload(project: Path, python: str, *args: str) -> subprocess.CompletedProcess[str]:
	environment = {**os.environ, "SQLLOAD_MOTHERDUCK_PYTHON": python}
	for name in ("MOTHERDUCK_TOKEN", "motherduck_token"):
		environment.pop(name, None)
	done = subprocess.run(
		[sys.executable, "-c", "from sqlload._cli import main; main()", *args], cwd=project, env=environment, capture_output=True, text=True
	)
	assert os.environ[TOKEN_VARIABLE] not in done.stdout + done.stderr
	return done


def load(project: Path, python: str) -> str:
	done = sqlload(project, python, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	return done.stdout


def give(project: Path, rows: dict[str, list[dict[str, Any]]]) -> None:
	for table, table_rows in rows.items():
		(project / "data" / f"{table}.json").write_text(json.dumps(table_rows))


def test_given_a_live_account_when_loading_twice_and_rerunning_then_every_strategy_and_type_lands(
	tmp_path: Path, python: str, database: str
) -> None:
	for folder in ("sources", "data"):
		(tmp_path / folder).mkdir()
	(tmp_path / "sqlload.toml").write_text(PROJECT.format(database=database, variable=TOKEN_VARIABLE))
	(tmp_path / "sources" / "shop.py").write_text(textwrap.dedent(MODULE))
	typed = {
		"flag": True, "small": 7, "big": 9007199254740993, "amount": "12.50", "ratio": 0.25, "label": "café ☕", "day": "2026-11-12",
		"at_utc": "2026-11-12T09:30:00.654321Z", "at_local": "2026-11-12T09:30:01", "doc": {"k": [1, "two"]},
		"uid": "0f8fad5b-d9cb-469f-a165-70867728950e", "tags": ["a", "b"],
	}
	give(tmp_path, {
		"events": [{"id": 1, "at": "2026-01-01T10:00:00Z"}, {"id": 2, "at": "2026-01-02T10:00:00Z"}],
		"customers": [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
		"days": [{"id": 1, "v": "a", "day": "2026-01-01T00:00:00Z"}, {"id": 2, "v": "b", "day": "2026-01-02T00:00:00Z"}],
		"snapshot": [{"id": 1}, {"id": 2}],
		"typed": [typed],
	})
	debug = sqlload(tmp_path, python, "debug")
	assert debug.returncode == 0, debug.stdout + debug.stderr
	assert f"MotherDuck {database}, DuckDB v1.5.6" in debug.stdout
	load(tmp_path, python)
	give(tmp_path, {
		"events": [{"id": 2, "at": "2026-01-02T10:00:00Z"}, {"id": 3, "at": "2026-01-03T10:00:00Z", "note": "new column"}],
		"customers": [{"id": 2, "name": "B"}, {"id": 3, "name": "c"}],
		"days": [{"id": 2, "v": "B", "day": "2026-01-02T00:00:00Z"}, {"id": 3, "v": "c", "day": "2026-01-03T00:00:00Z"}],
		"snapshot": [{"id": 3}],
	})
	load(tmp_path, python)
	load(tmp_path, python)
	tables = f"{database}.raw"
	assert admin(
		python,
		f"SELECT id, note FROM {tables}.cloud__events ORDER BY id",
	) == [[1, None], [2, None], [3, "new column"]]
	assert admin(python, f"SELECT id, name FROM {tables}.cloud__customers ORDER BY id") == [[1, "a"], [2, "B"], [3, "c"]]
	assert admin(python, f"SELECT id, v FROM {tables}.cloud__days ORDER BY id") == [[1, "a"], [2, "B"], [3, "c"]]
	assert admin(python, f"SELECT id FROM {tables}.cloud__snapshot ORDER BY id") == [[3]]
	assert admin(
		python,
		"SET TimeZone = 'UTC'",
		f"SELECT flag, small, big, CAST(amount AS VARCHAR), ratio, label, CAST(day AS VARCHAR), CAST(at_utc AS VARCHAR), "
		f"CAST(at_local AS VARCHAR), CAST(doc AS VARCHAR), CAST(uid AS VARCHAR), tags FROM {tables}.cloud__typed",
	) == [[
		True, 7, 9007199254740993, "12.50", 0.25, "café ☕", "2026-11-12", "2026-11-12 09:30:00.654321+00", "2026-11-12 09:30:01",
		'{"k":[1,"two"]}', "0f8fad5b-d9cb-469f-a165-70867728950e", ["a", "b"],
	]]
	assert admin(
		python,
		f"SELECT count(*) FROM duckdb_tables() WHERE database_name = '{database}' AND table_name LIKE '%sqlload_staging%'",
	) == [[0]]
