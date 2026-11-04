"""Python sources end to end: `sqlload` commands on projects with python_paths, into DuckDB and a files target."""

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.dataset as ds
import pytest

DUCKDB_PROJECT = """\
python_paths = ["sources"]

[connections.wh]
kind = "duckdb"
path = "wh.duckdb"

[connections.shop]
kind = "http"
base_url = "https://api.shop.example"
token = "${SHOP_TOKEN}"

[targets.dev]
connection = "wh"
loader_schema = "RAW"
"""

FILES_PROJECT = """\
python_paths = ["sources"]

[connections.lake]
kind = "filesystem"
root = "lake"

[connections.shop]
kind = "http"
base_url = "https://api.shop.example"

[targets.lake]
connection = "lake"
path = "{table}/"
"""

#: A module whose tables yield the rows in data/<table>.json and record their context in seen/<table>.json.
SHOP_MODULE = '''
import json
from pathlib import Path

import sqlload as sl

HERE = Path(__file__).resolve().parent.parent


def rows(ctx):
    seen = {
        "cursor_value": repr(ctx.cursor_value),
        "cursor_end": repr(ctx.cursor_end),
        "cursor_column": ctx.cursor_column,
        "is_reload": ctx.is_reload,
        "token": ctx.connection.get("token"),
    }
    (HERE / "seen").mkdir(exist_ok=True)
    (HERE / "seen" / f"{ctx.table}.json").write_text(json.dumps(seen))
    data = json.loads((HERE / "data" / f"{ctx.table}.json").read_text())
    if isinstance(data, dict) and "raise" in data:
        yield data["rows"][0]
        raise RuntimeError(data["raise"])
    yield from data


shop = sl.source("shop", connection="shop")

{tables}
'''


def make_project(tmp_path: Path, toml: str, tables: str, module: str = SHOP_MODULE) -> Path:
	(tmp_path / "sources").mkdir()
	(tmp_path / "data").mkdir()
	(tmp_path / "lake").mkdir()
	(tmp_path / "sqlload.toml").write_text(toml)
	(tmp_path / "sources" / "shop.py").write_text(module.replace("{tables}", textwrap.dedent(tables)))
	return tmp_path


def give(project: Path, table: str, rows: Any) -> None:
	(project / "data" / f"{table}.json").write_text(json.dumps(rows))


def sqlload(project: Path, *args: str) -> subprocess.CompletedProcess[str]:
	home = project / "home"
	home.mkdir(exist_ok=True)
	env = {**os.environ, "HOME": str(home), "XDG_CONFIG_HOME": str(home / ".config"), "XDG_CACHE_HOME": str(home / ".cache"), "SHOP_TOKEN": "t0ken"}
	return subprocess.run(
		[sys.executable, "-c", "from sqlload._cli import main; main()", *args],
		cwd=project,
		env=env,
		capture_output=True,
		text=True,
	)


def load(project: Path, *args: str) -> subprocess.CompletedProcess[str]:
	done = sqlload(project, "load", *args)
	assert done.returncode == 0, done.stdout + done.stderr
	return done


def query(project: Path, sql: str) -> list[tuple[Any, ...]]:
	with duckdb.connect(str(project / "wh.duckdb"), read_only=True) as connection:
		return connection.execute(sql).fetchall()


def seen(project: Path, table: str) -> dict[str, Any]:
	return json.loads((project / "seen" / f"{table}.json").read_text())


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
def test_given_a_python_table_when_loaded_twice_into_duckdb_then_its_strategy_applies(
	tmp_path: Path, settings: str, first: list[dict[str, Any]], second: list[dict[str, Any]], expected: list[tuple[Any, ...]]
) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, f"shop.table({settings})(rows)\n")
	give(project, "rows", first)
	load(project)
	give(project, "rows", second)
	load(project)
	assert query(project, 'SELECT id, v FROM "RAW"."rows" ORDER BY id') == expected
	assert query(project, 'SELECT count(DISTINCT _sqlload_load_id) FROM "RAW"."rows"')[0][0] >= 1


def test_given_new_fields_when_loading_again_then_columns_are_added_and_values_fit_the_live_types(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(write_strategy='append')(rows)\n")
	give(project, "rows", [{"id": 1, "code": "x1", "meta": {"k": 1}}])
	load(project)
	give(project, "rows", [{"id": 2, "code": 7, "meta": {"k": 2}, "extra": True}])
	done = load(project)
	assert "loaded  RAW.rows  1 rows" in done.stdout
	columns = query(project, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'rows' ORDER BY ordinal_position")
	assert [name for name, _ in columns] == ["id", "code", "meta", "_sqlload_load_id", "_sqlload_loaded_at", "extra"]
	assert query(project, 'SELECT id, code, meta, extra FROM "RAW"."rows" ORDER BY id') == [
		(1, "x1", '{"k":1}', None),
		(2, "7", '{"k":2}', True),
	]


def test_given_an_incremental_table_when_loading_then_ctx_carries_the_cursor_window(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(write_strategy='merge', unique_key='id', cursor_column='at', lookback='1h')(rows)\n")
	give(project, "rows", [{"id": 1, "at": "2026-01-01T10:00:00Z"}, {"id": 2, "at": "2026-01-02T10:00:00Z"}])
	load(project)
	assert seen(project, "rows")["cursor_value"] == "None"
	assert seen(project, "rows")["token"] == "t0ken"
	give(project, "rows", [{"id": 3, "at": "2026-01-03T10:00:00Z"}])
	load(project)
	assert seen(project, "rows")["cursor_value"] == "datetime.datetime(2026, 1, 2, 9, 0, tzinfo=datetime.timezone.utc)"
	assert seen(project, "rows")["cursor_column"] == "at"
	load(project, "--start-cursor-ts", "2025-12-01T00:00:00Z", "--end-cursor-ts", "2026-02-01T00:00:00Z")
	assert seen(project, "rows")["cursor_value"] == "datetime.datetime(2025, 12, 1, 0, 0, tzinfo=datetime.timezone.utc)"
	assert seen(project, "rows")["cursor_end"] == "datetime.datetime(2026, 2, 1, 0, 0, tzinfo=datetime.timezone.utc)"
	assert query(project, 'SELECT id FROM "RAW"."rows" ORDER BY id') == [(1,), (2,), (3,)]


def test_given_a_generator_that_raises_when_loading_then_only_its_table_fails_and_nothing_is_left_behind(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(name='good')(rows)\nshop.table(name='bad')(rows)\n")
	give(project, "good", [{"id": 1}])
	give(project, "bad", {"rows": [{"id": 1}], "raise": "the API returned 503"})
	done = sqlload(project, "load")
	assert done.returncode == 1
	assert "loaded  RAW.good  1 rows" in done.stdout + done.stderr
	assert "failed  RAW.bad  RuntimeError: the API returned 503" in done.stdout + done.stderr
	assert "shop.py" in done.stderr
	assert query(project, "SELECT table_name FROM information_schema.tables ORDER BY table_name") == [("good",)]


def test_given_python_tables_when_planning_then_the_plan_says_where_columns_come_from(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(name='orders', columns={'total': 'decimal(18,2)'})(rows)\nshop.table(name='stores')(rows)\n")
	done = sqlload(project, "plan")
	assert done.returncode == 0, done.stderr
	for line in [
		"create table (1 declared columns; the rest inferred on first load)",
		"create table on first load (columns inferred from the data)",
		"source  python  sources/shop.py (shop)",
		"columns inferred on first load, 1 declared",
	]:
		assert line in done.stdout, done.stdout
	give(project, "orders", [{"id": 1, "total": 2.5}])
	give(project, "stores", [{"id": 1}])
	load(project)
	assert query(project, "SELECT data_type FROM information_schema.columns WHERE table_name = 'orders' AND column_name = 'total'") == [("DECIMAL(18,2)",)]
	assert "columns inferred at load and checked against the live table" in sqlload(project, "plan", "stores").stdout


@pytest.mark.parametrize(
	("module", "message"),
	[
		("import sqlload as sl\nraise ValueError('broken module')\n", "cannot import sources/shop.py: ValueError: broken module"),
		("import sqlload as sl\n@sl.table(uniq_key='id')\ndef t(ctx):\n    yield {}\n", "unknown table setting(s): uniq_key"),
		("import sqlload as sl\n@sl.table(write_strategy='upsert')\ndef t(ctx):\n    yield {}\n", "unknown variant `upsert`"),
		("import sqlload as sl\na = sl.source('a')\nb = sl.source('b')\na.table(name='t')(lambda ctx: [])\nb.table(name='t')(lambda ctx: [])\n", "Python table t is declared twice"),
		("import sqlload as sl\nshop = sl.source('shop', connection='nope')\n", "Python source shop (sources/shop.py) connection \"nope\" is not a [connections] entry"),
	],
	ids=["import error", "unknown setting", "bad value", "duplicate table", "unknown connection"],
)
def test_given_bad_python_sources_when_planning_then_the_project_fails_closed(tmp_path: Path, module: str, message: str) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "", module=module)
	done = sqlload(project, "plan")
	assert done.returncode == 1
	assert message in done.stderr, done.stderr


def test_given_a_toml_python_source_when_parsing_then_it_is_rejected(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT + '\n[sources.shop]\nkind = "python"\n', "")
	done = sqlload(project, "debug", "--no-connection")
	assert done.returncode == 1
	assert "Python sources are declared in Python with sl.source" in done.stderr


def test_given_a_selector_when_loading_then_only_the_named_python_table_loads(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(name='a')(rows)\nshop.table(name='b')(rows)\n")
	give(project, "a", [{"id": 1}])
	give(project, "b", [{"id": 1}])
	done = load(project, "a")
	assert "RAW.a" in done.stdout and "RAW.b" not in done.stdout


def files_rows(project: Path, table: str) -> list[tuple[Any, ...]]:
	rows = ds.dataset(project / "lake" / table, format="parquet").to_table().to_pylist()
	return sorted((row["id"], row["v"]) for row in rows)


@pytest.mark.parametrize(
	("settings", "second", "expected"),
	[
		("write_strategy='append'", [{"id": 3, "v": "c"}], [(1, "a"), (2, "b"), (3, "c")]),
		("write_strategy='replace'", [{"id": 3, "v": "c"}], [(3, "c")]),
	],
	ids=["append", "replace"],
)
def test_given_a_files_target_when_loading_a_python_table_then_parquet_files_follow_the_strategy(
	tmp_path: Path, settings: str, second: list[dict[str, Any]], expected: list[tuple[Any, ...]]
) -> None:
	project = make_project(tmp_path, FILES_PROJECT, f"shop.table({settings})(rows)\n")
	give(project, "rows", [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}])
	load(project)
	give(project, "rows", second)
	load(project)
	assert files_rows(project, "rows") == expected


def test_given_a_files_target_with_day_partitions_when_delete_insert_reloads_a_day_then_it_is_replaced(tmp_path: Path) -> None:
	toml = FILES_PROJECT.replace('path = "{table}/"', 'path = "{table}/day={cursor:date}/"')
	project = make_project(tmp_path, toml, "shop.table(write_strategy='delete_insert', cursor_column='day', columns={'day': 'date'})(rows)\n")
	give(project, "rows", [{"id": 1, "v": "a", "day": "2026-01-01"}, {"id": 2, "v": "b", "day": "2026-01-02"}])
	load(project)
	give(project, "rows", [{"id": 1, "v": "stale", "day": "2026-01-01"}, {"id": 2, "v": "B", "day": "2026-01-02"}, {"id": 3, "v": "c", "day": "2026-01-03"}])
	load(project)
	rows = ds.dataset(project / "lake" / "rows", format="parquet").to_table().to_pylist()
	assert sorted((row["id"], row["v"]) for row in rows) == [(1, "a"), (2, "B"), (3, "c")]


def test_given_masked_and_excluded_columns_when_loading_then_values_are_hashed_or_dropped(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(mask={'email': 'hash'}, exclude_columns=['secret*'])(rows)\n")
	give(project, "rows", [{"id": 1, "email": "a@example.com", "secret_note": "x"}])
	load(project)
	(email,) = query(project, 'SELECT email FROM "RAW"."rows"')[0]
	assert len(email) == 64 and email != "a@example.com"
	columns = [name for (name,) in query(project, "SELECT column_name FROM information_schema.columns WHERE table_name = 'rows'")]
	assert "secret_note" not in columns


def test_given_a_type_the_live_column_cannot_hold_when_loading_then_the_table_fails_with_the_fix(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(write_strategy='append')(rows)\n")
	give(project, "rows", [{"id": 1, "amount": 1}])
	load(project)
	give(project, "rows", [{"id": 2, "amount": 2.5}])
	done = sqlload(project, "load")
	assert done.returncode == 1
	assert "failed  RAW.rows  the yielded data does not fit the table" in done.stderr, done.stderr
	assert query(project, 'SELECT id, amount FROM "RAW"."rows"') == [(1, 1)]


CLIENT_MODULE = '''
from pathlib import Path

import sqlload as sl

HERE = Path(__file__).resolve().parent.parent
shop = sl.source("shop", connection="shop")


class Client:
    def __init__(self, base_url):
        self.base_url = base_url
        with (HERE / "events.txt").open("a") as events:
            events.write("open\\n")

    def close(self):
        with (HERE / "events.txt").open("a") as events:
            events.write("close\\n")


@shop.client
def client(ctx):
    return Client(ctx.connection["base_url"])


@shop.table(name="a")
def a(ctx):
    yield {"url": ctx.client.base_url}


@shop.table(name="b")
def b(ctx):
    yield {"url": ctx.client.base_url, "first": ctx.cursor_value}
'''


def test_given_a_source_client_when_its_tables_load_then_it_is_built_once_and_closed_at_the_end(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "", module=CLIENT_MODULE)
	load(project)
	assert (project / "events.txt").read_text() == "open\nclose\n"
	assert query(project, 'SELECT url FROM "RAW"."a"') == [("https://api.shop.example",)]
