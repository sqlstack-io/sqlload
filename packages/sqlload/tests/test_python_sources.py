"""Python sources end to end: `sqlload` commands on projects with python_paths, into DuckDB and a files target."""

import json
import os
import subprocess
import sys
import textwrap
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.dataset as ds
import pytest

DUCKDB_PROJECT = """\
python_paths = ["sources"]

[connections.wh]
kind = "duckdb"
database = "wh.duckdb"

[connections.shop]
kind = "http"
base_url = "https://api.shop.example"
token = "${ENV:SHOP_TOKEN}"

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


def target_connection(ctx):
    try:
        return dict(ctx.connection("wh"))
    except (PermissionError, KeyError) as error:
        return f"{type(error).__name__}: {error}"


def rows(ctx):
    seen = {
        "cursor_value": repr(ctx.cursor_value),
        "cursor_end": repr(ctx.cursor_end),
        "cursor_column": ctx.cursor_column,
        "is_reload": ctx.is_reload,
        "token": ctx.connection.get("token"),
        "other": dict(ctx.connection("shop")),
        "target": target_connection(ctx),
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
	assert query(project, 'SELECT id, v FROM "RAW"."shop__rows" ORDER BY id') == expected
	assert query(project, 'SELECT count(DISTINCT _sqlload_load_id) FROM "RAW"."shop__rows"')[0][0] >= 1


def test_given_new_fields_when_loading_again_then_columns_are_added_and_values_fit_the_live_types(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(write_strategy='append')(rows)\n")
	give(project, "rows", [{"id": 1, "code": "x1", "meta": {"k": 1}}])
	load(project)
	give(project, "rows", [{"id": 2, "code": 7, "meta": {"k": 2}, "extra": True}])
	done = load(project)
	assert "loaded   RAW.shop__rows  1 rows" in done.stdout
	columns = query(project, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'shop__rows' ORDER BY ordinal_position")
	assert [name for name, _ in columns] == ["id", "code", "meta", "_sqlload_load_id", "_sqlload_loaded_at", "extra"]
	assert query(project, 'SELECT id, code, meta, extra FROM "RAW"."shop__rows" ORDER BY id') == [
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
	load(project, "--start-cursor", "2025-12-01T00:00:00Z", "--end-cursor", "2026-02-01T00:00:00Z")
	assert seen(project, "rows")["cursor_value"] == "datetime.datetime(2025, 12, 1, 0, 0, tzinfo=datetime.timezone.utc)"
	assert seen(project, "rows")["cursor_end"] == "datetime.datetime(2026, 2, 1, 0, 0, tzinfo=datetime.timezone.utc)"
	assert query(project, 'SELECT id FROM "RAW"."shop__rows" ORDER BY id') == [(1,), (2,), (3,)]


def test_given_a_generator_that_raises_when_loading_then_only_its_table_fails_and_nothing_is_left_behind(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(name='good')(rows)\nshop.table(name='bad')(rows)\n")
	give(project, "good", [{"id": 1}])
	give(project, "bad", {"rows": [{"id": 1}], "raise": "the API returned 503"})
	done = sqlload(project, "load")
	assert done.returncode == 1
	assert "loaded   RAW.shop__good  1 rows" in done.stdout + done.stderr
	assert "failed   RAW.shop__bad   RuntimeError: the API returned 503" in done.stdout + done.stderr
	assert "shop.py" in done.stdout + done.stderr
	assert query(project, "SELECT table_name FROM information_schema.tables ORDER BY table_name") == [("shop__good",)]


def test_given_python_tables_when_planning_then_the_plan_says_where_columns_come_from(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(name='orders', columns={'total': 'decimal(18,2)'})(rows)\nshop.table(name='stores')(rows)\n")
	done = sqlload(project, "plan")
	assert done.returncode == 0, done.stderr
	for line in [
		"create table on first load (1 declared columns, the rest inferred from the data)",
		"create table on first load (columns inferred from the data)",
		"source  python  sources/shop.py (shop)",
		"columns inferred on first load, 1 declared",
	]:
		assert line in done.stdout, done.stdout
	give(project, "orders", [{"id": 1, "total": 2.5}])
	give(project, "stores", [{"id": 1}])
	load(project)
	assert query(project, "SELECT data_type FROM information_schema.columns WHERE table_name = 'shop__orders' AND column_name = 'total'") == [("DECIMAL(18,2)",)]
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
	assert message in done.stdout + done.stderr, done.stderr


def test_given_a_toml_python_source_when_parsing_then_it_is_rejected(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT + '\n[sources.shop]\nkind = "python"\n', "")
	done = sqlload(project, "debug", "--no-connection")
	assert done.returncode == 1
	assert "Python sources are declared in Python with sl.source" in done.stdout + done.stderr


def test_given_a_selector_when_loading_then_only_the_named_python_table_loads(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(name='a')(rows)\nshop.table(name='b')(rows)\n")
	give(project, "a", [{"id": 1}])
	give(project, "b", [{"id": 1}])
	done = load(project, "a")
	assert "RAW.shop__a" in done.stdout and "RAW.shop__b" not in done.stdout


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
	(email,) = query(project, 'SELECT email FROM "RAW"."shop__rows"')[0]
	assert len(email) == 64 and email != "a@example.com"
	columns = [name for (name,) in query(project, "SELECT column_name FROM information_schema.columns WHERE table_name = 'shop__rows'")]
	assert "secret_note" not in columns


PRICED_TABLE = """
import decimal


def priced(ctx):
    for row in rows(ctx):
        amount = row["amount"]
        yield {**row, "amount": decimal.Decimal(amount) if isinstance(amount, str) else amount}


shop.table(write_strategy='append'{settings})(priced)
"""


@pytest.mark.parametrize(
	("settings", "amount", "expected_code", "expected_line", "expected_rows"),
	[
		(
			"",
			"2.50",
			1,
			"failed   RAW.shop__priced  the yielded data does not fit the table: amount is int in the table but decimal(38,2) in the source; pin a type the table can hold, for example columns = { amount = \"int\" }; or set auto_widen = true to widen it",
			[(1, 1)],
		),
		(
			", auto_widen=True",
			"2.50",
			0,
			"altered  RAW.shop__priced  widen  amount int -> decimal(38,2) (DECIMAL(38,2))  in place",
			[(1, Decimal("1.00")), (2, Decimal("2.50"))],
		),
		(
			", auto_widen=True",
			2.5,
			1,
			"failed   RAW.shop__priced  the yielded data does not fit the table: amount is int in the table but float in the source",
			[(1, 1)],
		),
	],
	ids=["fails-without-auto-widen", "auto-widen-to-decimal", "never-to-float"],
)
def test_given_an_inferred_column_that_outgrows_the_table_when_loading_then_auto_widen_decides(
	tmp_path: Path, settings: str, amount: Any, expected_code: int, expected_line: str, expected_rows: list[tuple[Any, ...]]
) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, PRICED_TABLE.replace("{settings}", settings))
	give(project, "priced", [{"id": 1, "amount": 1}])
	load(project)
	give(project, "priced", [{"id": 2, "amount": amount}])
	done = sqlload(project, "load")
	assert done.returncode == expected_code, done.stdout + done.stderr
	assert expected_line in done.stdout, done.stdout + done.stderr
	assert query(project, 'SELECT id, amount FROM "RAW"."shop__priced" ORDER BY id') == expected_rows


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
	assert query(project, 'SELECT url FROM "RAW"."shop__a"') == [("https://api.shop.example",)]


def test_given_a_live_table_with_upper_case_columns_when_loading_again_then_columns_match_and_the_live_cursor_type_is_used(tmp_path: Path) -> None:
	project = make_project(
		tmp_path, DUCKDB_PROJECT, "shop.table(write_strategy='merge', unique_key='seq', cursor_column='updated_at', lookback='1h')(rows)\n"
	)
	with duckdb.connect(str(project / "wh.duckdb")) as connection:
		connection.execute('CREATE SCHEMA "RAW"')
		connection.execute('CREATE TABLE "RAW"."shop__rows" ("SEQ" BIGINT, "KIND" VARCHAR, "UPDATED_AT" TIMESTAMPTZ)')
		connection.execute("INSERT INTO \"RAW\".\"shop__rows\" VALUES (1, 'a', '2026-01-02 10:00:00+00')")
	give(project, "rows", [{"seq": 1, "kind": "A", "updated_at": "2026-01-02T10:00:00Z"}, {"seq": 2, "kind": "b", "updated_at": "2026-01-03T10:00:00Z"}])
	load(project)
	assert seen(project, "rows")["cursor_value"] == "datetime.datetime(2026, 1, 2, 9, 0, tzinfo=datetime.timezone.utc)"
	assert query(project, 'SELECT "SEQ", "KIND" FROM "RAW"."shop__rows" ORDER BY 1') == [(1, "A"), (2, "b")]
	columns = query(project, "SELECT column_name FROM information_schema.columns WHERE table_name = 'shop__rows' ORDER BY ordinal_position")
	assert [name for (name,) in columns] == ["SEQ", "KIND", "UPDATED_AT", "_sqlload_load_id", "_sqlload_loaded_at"]


def test_given_a_column_that_is_null_in_every_row_when_loading_again_then_it_takes_the_live_type(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(write_strategy='append')(rows)\n")
	give(project, "rows", [{"id": 1, "n": 5}])
	load(project)
	give(project, "rows", [{"id": 2, "n": None}])
	load(project)
	assert query(project, 'SELECT id, n FROM "RAW"."shop__rows" ORDER BY id') == [(1, 5), (2, None)]


@pytest.mark.parametrize(
	"settings",
	[
		"columns={'total': 'decimal(18,2)'}, on_schema_change='fail'",
		"columns={'total': 'decimal(18,2)'}, on_schema_change='ignore'",
		"write_strategy='append', cursor_column='at', columns={'total': 'decimal(18,2)'}",
	],
	ids=["fail", "ignore", "append with a cursor"],
)
def test_given_declared_columns_when_applying_then_the_first_load_creates_the_whole_table(tmp_path: Path, settings: str) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, f"shop.table({settings})(rows)\n")
	applied = sqlload(project, "apply")
	assert applied.returncode == 0, applied.stderr
	assert query(project, "SELECT count(*) FROM information_schema.tables WHERE table_name = 'shop__rows'") == [(0,)]
	give(project, "rows", [{"id": 1, "total": 2.5, "at": "2026-01-01T00:00:00Z"}])
	done = load(project)
	assert "created  RAW.shop__rows  5 columns" in done.stdout
	assert query(project, 'SELECT id, total, epoch("at") FROM "RAW"."shop__rows"') == [(1, Decimal("2.50"), 1767225600.0)]


def test_given_new_fields_when_a_python_table_loads_then_the_changes_are_listed(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(write_strategy='append')(rows)\n")
	give(project, "rows", [{"id": 1}])
	load(project)
	give(project, "rows", [{"id": 2, "extra": "x"}])
	done = load(project)
	assert "altered  RAW.shop__rows  add column  extra text -> VARCHAR" in done.stdout, done.stdout


@pytest.mark.parametrize(
	("settings", "rows", "message"),
	[
		("mask={'Email': 'hash'}", [{"id": 1, "email": "a@example.com"}], "mask key Email names no column of the data; did you mean email?"),
		("mask={'ssn': 'hash'}", [{"id": 1, "SSN": "123"}], "mask key ssn names no column of the data; did you mean SSN?"),
		("mask={'ssn': 'hash'}", [{"id": 1}], "mask ssn names no column of the data"),
		("columns={'totl': 'int'}", [{"id": 1, "total": 1}], "columns key totl names no column of the data; did you mean total?"),
	],
	ids=["mask case", "mask upper case data", "mask missing", "columns typo"],
)
def test_given_a_declared_key_the_data_does_not_have_when_loading_then_the_table_fails_and_nothing_loads_unmasked(
	tmp_path: Path, settings: str, rows: list[dict[str, Any]], message: str
) -> None:
	toml = DUCKDB_PROJECT.replace('[targets.dev]', '[connections.keys]\nkind = "http"\nbase_url = "x"\n\n[targets.dev]')
	project = make_project(tmp_path, toml, f"shop.table({settings})(rows)\n")
	give(project, "rows", rows)
	done = sqlload(project, "load")
	assert done.returncode == 1
	assert message in done.stdout + done.stderr, done.stderr
	assert query(project, "SELECT count(*) FROM information_schema.tables WHERE table_name = 'shop__rows'") == [(0,)]


@pytest.mark.parametrize(
	("settings", "expected_line", "expected_tables"),
	[
		("", "loaded   RAW.shop__rows  0 rows (table created on first data)", []),
		("columns={'id': 'int'}", "loaded   RAW.shop__rows  0 rows", [("id", "BIGINT")]),
	],
	ids=["nothing declared", "declared columns"],
)
def test_given_a_generator_that_yields_nothing_on_its_first_load_then_the_load_succeeds(
	tmp_path: Path, settings: str, expected_line: str, expected_tables: list[tuple[str, str]]
) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, f"shop.table({settings})(rows)\n")
	give(project, "rows", [])
	done = load(project)
	assert expected_line in done.stdout, done.stdout
	columns = query(
		project,
		"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'shop__rows' AND column_name NOT LIKE '\\_sqlload%' ESCAPE '\\'",
	)
	assert columns == expected_tables


def test_given_a_files_target_and_an_undeclared_cursor_when_planning_then_it_asks_to_declare_the_cursor(tmp_path: Path) -> None:
	toml = FILES_PROJECT.replace('path = "{table}/"', 'path = "{table}/day={cursor:date}/"')
	project = make_project(tmp_path, toml, "shop.table(write_strategy='delete_insert', cursor_column='day')(rows)\n")
	done = sqlload(project, "plan")
	assert done.returncode == 1
	assert 'files targets need its type before the first load: declare it, for example columns={"day": "date"}' in done.stdout + done.stderr, done.stderr


@pytest.mark.parametrize(
	("column_type", "first", "second", "expected"),
	[
		("decimal(18,2)", ["1.50", "2.50"], ["1.50", "2.50", "3.50"], [Decimal("1.50"), Decimal("2.50"), Decimal("3.50")]),
		("smallint", [1, 2], [1, 2, 3], [1, 2, 3]),
	],
	ids=["decimal", "smallint"],
)
def test_given_a_decimal_or_smallint_cursor_when_appending_again_then_only_new_rows_load(
	tmp_path: Path, column_type: str, first: list[Any], second: list[Any], expected: list[Any]
) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, f"shop.table(write_strategy='append', cursor_column='v', append_cursor_inclusive=False, columns={{'v': '{column_type}'}})(rows)\n")
	give(project, "rows", [{"v": value} for value in first])
	load(project)
	give(project, "rows", [{"v": value} for value in second])
	load(project)
	assert [value for (value,) in query(project, 'SELECT v FROM "RAW"."shop__rows" ORDER BY v')] == expected


def test_given_cursor_start_when_the_cursor_type_is_not_known_yet_then_ctx_gets_the_same_type_on_every_run(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(write_strategy='append', cursor_column='at', cursor_start='2026-01-01T00:00:00Z')(rows)\n")
	give(project, "rows", [])
	load(project)
	assert seen(project, "rows")["cursor_value"] == "datetime.datetime(2026, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)"
	give(project, "rows", [{"id": 1, "at": "2026-01-05T00:00:00Z"}])
	load(project)
	assert seen(project, "rows")["cursor_value"] == "datetime.datetime(2026, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)"
	load(project)
	assert seen(project, "rows")["cursor_value"] == "datetime.datetime(2026, 1, 5, 0, 0, tzinfo=datetime.timezone.utc)"


def test_given_a_target_connection_when_a_table_function_asks_for_it_then_it_is_withheld(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table()(rows)\n")
	give(project, "rows", [{"id": 1}])
	load(project)
	assert seen(project, "rows")["other"]["base_url"] == "https://api.shop.example"
	assert seen(project, "rows")["target"].startswith("PermissionError: connection wh is used by a [targets] entry")


NAMED_MODULE = """
import sqlload as sl

shop = sl.source("shop", table_name="api_{table}")


@shop.table(name="orders")
def orders(ctx):
    yield {"id": 1}


@shop.table(name="stores", table_name="{source}_stores_v2")
def stores(ctx):
    yield {"id": 1}


@sl.table
def standalone(ctx):
    yield {"id": 1}
"""


def test_given_table_name_settings_when_loading_then_tables_are_named_from_them(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "", module=NAMED_MODULE)
	load(project)
	tables = query(project, "SELECT table_name FROM information_schema.tables ORDER BY table_name")
	assert tables == [("api_orders",), ("shop_stores_v2",), ("standalone",)]


@pytest.mark.parametrize("entry", [".", "..", "sources/../.."], ids=["root", "parent", "escape"])
def test_given_a_python_path_outside_the_project_folder_when_planning_then_discovery_refuses_it(tmp_path: Path, entry: str) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT.replace('python_paths = ["sources"]', f"python_paths = [{entry!r}]".replace("'", '"')), "")
	done = sqlload(project, "plan")
	assert done.returncode == 1
	assert "DiscoveryError\n    └── reason  python_paths entry" in done.stdout + done.stderr, done.stderr
	assert "must be a folder inside the project folder" in done.stdout + done.stderr


def test_given_environments_and_build_folders_under_python_paths_when_discovering_then_they_are_skipped(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "")
	for folder in [".venv", "venv", "build", "dist", "__pycache__", "lib/site-packages"]:
		(project / "sources" / folder).mkdir(parents=True)
		(project / "sources" / folder / "broken.py").write_text("raise RuntimeError('imported')\n")
	done = sqlload(project, "plan")
	assert done.returncode == 0, done.stderr


@pytest.mark.parametrize(
	("settings", "args", "fails", "expected_rows"),
	[
		("write_strategy='replace'", (), True, [(1,)]),
		("write_strategy='append'", ("--reload", "--yes"), True, [(1,)]),
		("write_strategy='replace', allow_empty=True", (), False, []),
		("write_strategy='append'", (), False, [(1,)]),
		("write_strategy='merge', unique_key='id'", (), False, [(1,)]),
	],
	ids=["replace", "reload", "replace allowed", "append", "merge"],
)
def test_given_no_rows_when_loading_into_a_table_with_rows_then_only_an_allowed_replace_empties_it(
	tmp_path: Path, settings: str, args: tuple[str, ...], fails: bool, expected_rows: list[tuple[Any, ...]]
) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, f"shop.table({settings})(rows)\n")
	give(project, "rows", [{"id": 1}])
	load(project)
	give(project, "rows", [])
	done = sqlload(project, "load", *args)
	assert (done.returncode == 1) == fails, done.stdout + done.stderr
	assert ("yielded no rows; replace would empty RAW.shop__rows; set allow_empty=True" in done.stdout + done.stderr) == fails, done.stderr
	assert query(project, 'SELECT id FROM "RAW"."shop__rows"') == expected_rows


@pytest.mark.parametrize(
	("settings", "fails", "expected_rows"),
	[
		("write_strategy='replace'", True, [(1, "a")]),
		("write_strategy='replace', allow_empty=True", False, []),
		("write_strategy='append'", False, [(1, "a")]),
	],
	ids=["replace", "replace allowed", "append"],
)
def test_given_no_rows_when_loading_into_files_with_rows_then_only_an_allowed_replace_empties_them(
	tmp_path: Path, settings: str, fails: bool, expected_rows: list[tuple[Any, ...]]
) -> None:
	project = make_project(tmp_path, FILES_PROJECT, f"shop.table({settings})(rows)\n")
	give(project, "rows", [{"id": 1, "v": "a"}])
	load(project)
	give(project, "rows", [])
	done = sqlload(project, "load")
	assert (done.returncode == 1) == fails, done.stdout + done.stderr
	assert ("yielded no rows; replace would empty" in done.stdout + done.stderr) == fails, done.stderr
	assert files_rows(project, "rows") == expected_rows


def test_given_allow_empty_on_a_source_with_a_bad_value_when_planning_then_the_setting_is_validated(tmp_path: Path) -> None:
	module = "import sqlload as sl\nshop = sl.source('shop', allow_empty='yes')\nshop.table(name='t')(lambda ctx: [])\n"
	project = make_project(tmp_path, DUCKDB_PROJECT, "", module=module)
	done = sqlload(project, "plan")
	assert done.returncode == 1
	assert "invalid type: string \"yes\", expected a boolean" in done.stdout + done.stderr, done.stderr


def test_given_allow_empty_in_sqlload_toml_when_parsing_then_it_is_an_unknown_key(tmp_path: Path) -> None:
	toml = DUCKDB_PROJECT + '\n[connections.pg]\nkind = "postgres"\nhost = "h"\nuser = "u"\ndatabase = "d"\n\n[sources.app]\nkind = "postgres"\nconnection = "pg"\ninclude = ["*"]\nallow_empty = true\n'
	project = make_project(tmp_path, toml, "")
	done = sqlload(project, "debug", "--no-connection")
	assert done.returncode == 1
	assert 'unknown key "allow_empty" in [sources.app]' in done.stdout + done.stderr, done.stderr


def test_given_a_python_source_on_a_warehouse_targets_connection_when_planning_then_the_project_is_refused(tmp_path: Path) -> None:
	module = "import sqlload as sl\nshop = sl.source('shop', connection='wh')\nshop.table(name='t')(lambda ctx: [])\n"
	project = make_project(tmp_path, DUCKDB_PROJECT, "", module=module)
	done = sqlload(project, "plan")
	assert done.returncode == 1
	assert "Python source shop uses connection wh, which target dev loads into; Python code never gets warehouse credentials" in done.stdout + done.stderr, done.stderr


LAKE_MODULE = """
import sqlload as sl

lake = sl.source("lake_files", connection="lake")


@lake.table(name="listing")
def listing(ctx):
    yield {"root": ctx.connection["root"]}
"""


def test_given_a_python_source_on_a_files_targets_connection_when_loading_then_it_reads_that_connection(tmp_path: Path) -> None:
	project = make_project(tmp_path, FILES_PROJECT, "", module=LAKE_MODULE)
	load(project)
	rows = ds.dataset(project / "lake" / "listing", format="parquet").to_table().to_pylist()
	assert [row["root"] for row in rows] == ["lake"]


def test_given_an_enforced_contract_without_declared_columns_when_planning_then_the_table_is_blocked(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(contract='enforced')(rows)\n")
	done = sqlload(project, "plan")
	assert done.returncode == 1
	assert "contract enforced needs declared columns: the first load would create a table with only metadata columns" in done.stdout + done.stderr
