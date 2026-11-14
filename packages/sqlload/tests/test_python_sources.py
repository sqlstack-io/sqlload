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
	project = make_project(tmp_path, DUCKDB_PROJECT, f"shop.table(description='Test table.', {settings})(rows)\n")
	give(project, "rows", first)
	load(project)
	give(project, "rows", second)
	load(project)
	assert query(project, 'SELECT id, v FROM "RAW"."shop__rows" ORDER BY id') == expected
	assert query(project, 'SELECT count(DISTINCT _sqlload_load_id) FROM "RAW"."shop__rows"')[0][0] >= 1


def test_given_dlt_naming_when_a_python_table_merges_twice_then_fields_load_snake_cased_and_merge_by_the_yielded_key(tmp_path: Path) -> None:
	project = make_project(
		tmp_path,
		DUCKDB_PROJECT,
		"shop.table(description='Test table.', naming='dlt', write_strategy='merge', unique_key='OrderID', unnest=['LineItems'])(rows)\n",
	)
	give(project, "rows", [{"OrderID": 1, "Unit Price": "a", "LineItems": [{"UnitPrice": 5}]}, {"OrderID": 2, "Unit Price": "b", "LineItems": []}])
	load(project)
	give(project, "rows", [{"OrderID": 1, "Unit Price": "c", "LineItems": [{"UnitPrice": 7}]}])
	load(project)
	columns = query(project, "SELECT column_name FROM information_schema.columns WHERE table_name = 'shop__rows' ORDER BY ordinal_position")
	assert [name for (name,) in columns] == ["order_id", "unit_price", "_dlt_load_id", "_sqlload_loaded_at", "_dlt_id"]
	assert query(project, 'SELECT order_id, unit_price FROM "RAW"."shop__rows" ORDER BY order_id') == [(1, "c"), (2, "b")]
	assert query(project, 'SELECT o.order_id, i.unit_price FROM "RAW"."shop__rows" o JOIN "RAW"."shop__rows__line_items" i ON i._dlt_parent_id = o._dlt_id') == [(1, 7)]


def test_given_dlt_naming_and_a_source_spelled_cursor_when_loading_again_then_ctx_gets_the_loaded_cursor_and_its_maximum(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', naming='dlt', write_strategy='append', cursor_column='UpdatedAt')(rows)\n")
	give(project, "rows", [{"ID": 1, "UpdatedAt": 10}, {"ID": 2, "UpdatedAt": 20}])
	load(project)
	give(project, "rows", [{"ID": 3, "UpdatedAt": 30}])
	load(project)
	assert seen(project, "rows")["cursor_column"] == "updated_at"
	assert seen(project, "rows")["cursor_value"] == "20"
	assert query(project, 'SELECT id, updated_at FROM "RAW"."shop__rows" ORDER BY id') == [(1, 10), (2, 20), (3, 30)]


def test_given_dlt_naming_when_two_yielded_fields_load_as_one_name_then_the_table_fails_naming_both(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', naming='dlt')(rows)\n")
	give(project, "rows", [{"OrderID": 1, "Order ID": 2}])
	done = sqlload(project, "load")
	assert done.returncode != 0
	assert 'fields "OrderID" and "Order ID" both load as order_id under naming = "dlt"' in done.stdout + done.stderr


def test_given_new_fields_when_loading_again_then_columns_are_added_and_values_fit_the_live_types(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', write_strategy='append')(rows)\n")
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
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', write_strategy='merge', unique_key='id', cursor_column='at', lookback='1h')(rows)\n")
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
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', name='good')(rows)\nshop.table(description='Test table.', name='bad')(rows)\n")
	give(project, "good", [{"id": 1}])
	give(project, "bad", {"rows": [{"id": 1}], "raise": "the API returned 503"})
	done = sqlload(project, "load")
	assert done.returncode == 1
	assert "loaded   RAW.shop__good  1 rows" in done.stdout + done.stderr
	assert "failed   RAW.shop__bad   RuntimeError: the API returned 503" in done.stdout + done.stderr
	assert "shop.py" in done.stdout + done.stderr
	assert query(project, "SELECT table_name FROM information_schema.tables ORDER BY table_name") == [("shop__good",)]



def test_given_one_table_failing_when_loading_with_json_then_stdout_is_one_document_with_each_tables_outcome(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', name='good')(rows)\nshop.table(description='Test table.', name='bad')(rows)\n")
	give(project, "good", [{"id": 1}, {"id": 2}])
	give(project, "bad", {"rows": [{"id": 1}], "raise": "the API returned 503"})
	done = sqlload(project, "load", "--json")
	assert done.returncode == 1, done.stdout + done.stderr
	result = json.loads(done.stdout)
	assert (result["schema_version"], result["command"], result["ok"], result["retryable"]) == (1, "load", False, False)
	assert result["summary"] == {"created": 1, "altered": 0, "recreated": 0, "loaded": 1, "failed": 1, "skipped": 0, "rows": 2}
	assert [(o["action"], o["name"]) for o in result["objects"]] == [("created", "RAW.shop__good")]
	outcomes = {table["table"]: table for table in result["tables"]}
	assert {name: (t["status"], t["rows"], t["source"], t["target"], t["write_strategy"]) for name, t in outcomes.items()} == {
		"RAW.shop__good": ("loaded", 2, "shop", "dev", "replace"),
		"RAW.shop__bad": ("failed", None, "shop", "dev", "replace"),
	}
	assert "RuntimeError: the API returned 503" in outcomes["RAW.shop__bad"]["error"]
	assert {name: t["retryable"] for name, t in outcomes.items()} == {"RAW.shop__good": False, "RAW.shop__bad": False}
	assert result["plan"]["selected"] == 2
	reload = sqlload(project, "load", "--json", "--reload")
	assert reload.returncode == 1, reload.stdout + reload.stderr
	error = json.loads(reload.stdout)["error"]
	assert (error["code"], "--json cannot ask to confirm it; add --yes" in error["message"]) == ("load", True), reload.stdout
	applied = sqlload(project, "apply", "--json")
	assert applied.returncode == 0, applied.stdout + applied.stderr
	assert (json.loads(applied.stdout)["command"], json.loads(applied.stdout)["tables"]) == ("apply", [])


#: Tables that raise what data/fail.json names: `pages` (logged, a checkpoint per page) at a page, `events` (a cursor) at a value, `broken` at all.
RETRY_MODULE = """
import json
from pathlib import Path

import sqlload as sl

HERE = Path(__file__).resolve().parent.parent
shop = sl.source("shop")


def failure():
    path = HERE / "data" / "fail.json"
    return json.loads(path.read_text()) if path.exists() else {}


@shop.table(description="Pages.", write_strategy="append", state="logged")
def pages(ctx):
    page = ctx.state or 0
    while page < 4:
        if failure().get("pages") == page:
            raise ConnectionError("the API dropped the connection")
        yield [{"n": page * 10 + row} for row in range(3)]
        page += 1
        yield sl.checkpoint(page)


@shop.table(description="Events.", write_strategy="append", cursor_column="at")
def events(ctx):
    for at in range((ctx.cursor_value or 0) + 1, 7):
        if failure().get("events") == at:
            raise TimeoutError("the read timed out")
        yield {"at": at}


@shop.table(description="Broken.")
def broken(ctx):
    if failure().get("broken"):
        raise ValueError("a row is malformed")
    yield {"id": 1}
"""


def test_given_tables_failing_only_on_the_network_when_loading_then_it_exits_75_and_a_rerun_loads_every_row_once(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "", module=RETRY_MODULE)
	give(project, "fail", {"pages": 2, "events": 4})
	assert sqlload(project, "load", "shop__pages", "shop__events").returncode == 75
	done = sqlload(project, "load", "--json", "shop__pages", "shop__events")
	assert done.returncode == 75, done.stdout + done.stderr
	result = json.loads(done.stdout)
	assert (result["ok"], result["retryable"]) == (False, True)
	assert {t["table"]: (t["status"], t["retryable"]) for t in result["tables"]} == {
		"RAW.shop__pages": ("failed", True),
		"RAW.shop__events": ("failed", True),
	}
	assert "ConnectionError: the API dropped the connection" in next(t["error"] for t in result["tables"] if t["table"] == "RAW.shop__pages")
	give(project, "fail", {})
	again = sqlload(project, "load", "--json", "shop__pages", "shop__events")
	assert again.returncode == 0, again.stdout + again.stderr
	assert (json.loads(again.stdout)["ok"], json.loads(again.stdout)["retryable"]) == (True, False)
	assert query(project, 'SELECT n FROM "RAW".shop__pages ORDER BY n') == [(page * 10 + row,) for page in range(4) for row in range(3)]
	assert query(project, 'SELECT "at" FROM "RAW".shop__events ORDER BY 1') == [(at,) for at in range(1, 7)]


def test_given_a_retryable_and_a_lasting_failure_when_loading_then_it_exits_1_and_says_which_is_which(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "", module=RETRY_MODULE)
	give(project, "fail", {"pages": 0, "broken": True})
	assert sqlload(project, "load", "shop__pages", "shop__broken").returncode == 1
	done = sqlload(project, "load", "--json", "shop__pages", "shop__broken")
	assert done.returncode == 1, done.stdout + done.stderr
	result = json.loads(done.stdout)
	assert (result["ok"], result["retryable"]) == (False, False)
	assert {t["table"]: t["retryable"] for t in result["tables"]} == {"RAW.shop__pages": True, "RAW.shop__broken": False}


def test_given_a_plan_error_and_a_printing_source_when_loading_with_json_then_stdout_parses_and_the_run_is_not_ok(tmp_path: Path) -> None:
	module = """import sqlload as sl

shop = sl.source("shop", connection="shop")


@shop.table(description="Orders.")
def orders(ctx):
    print("fetching page 1")
    yield {"order_id": 1}


@shop.table(description="Refunds.", columns={"refund_id": "no_such_type"})
def refunds(ctx):
    yield {"refund_id": 1}
"""
	project = make_project(tmp_path, DUCKDB_PROJECT, "", module=module)
	done = sqlload(project, "load", "--json")
	assert done.returncode == 1, done.stdout + done.stderr
	result = json.loads(done.stdout)
	assert (result["ok"], result["summary"]["loaded"], result["summary"]["failed"]) == (False, 1, 0)
	assert [error["name"] for error in result["plan"]["errors"]] == ["refunds"]
	assert "fetching page 1" in done.stderr

def test_given_python_tables_when_planning_then_the_plan_says_where_columns_come_from(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', name='orders', columns={'total': 'decimal(18,2)'})(rows)\nshop.table(description='Test table.', name='stores')(rows)\n")
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
		("import sqlload as sl\n@sl.table(description='Test table.', uniq_key='id')\ndef t(ctx):\n    yield {}\n", "unknown table setting(s): uniq_key"),
		("import sqlload as sl\n@sl.table(description='Test table.', write_strategy='upsert')\ndef t(ctx):\n    yield {}\n", "unknown variant `upsert`"),
		("import sqlload as sl\na = sl.source('a')\nb = sl.source('b')\na.table(description='Test table.', name='t')(lambda ctx: [])\nb.table(description='Test table.', name='t')(lambda ctx: [])\n", "Python table t is declared twice"),
		("import sqlload as sl\nshop = sl.source('shop', connection='nope')\n", "Python source shop (sources/shop.py) connection \"nope\" is not a [connections] entry"),
		(
			"import sqlload as sl\n@sl.table\ndef t(ctx):\n    yield {}\n",
			"t\n    └── reason  no description; add description=\"...\" to its table declaration, or a docstring to its function",
		),
	],
	ids=["import error", "unknown setting", "bad value", "duplicate table", "unknown connection", "no description"],
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
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', name='a')(rows)\nshop.table(description='Test table.', name='b')(rows)\n")
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
	project = make_project(tmp_path, FILES_PROJECT, f"shop.table(description='Test table.', {settings})(rows)\n")
	give(project, "rows", [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}])
	load(project)
	give(project, "rows", second)
	load(project)
	assert files_rows(project, "rows") == expected


def test_given_single_file_csv_when_columns_appear_partway_through_a_load_then_it_fails_and_keeps_the_table(tmp_path: Path) -> None:
	toml = FILES_PROJECT.replace('path = "{table}/"', 'path = "{table}/"\nformat = "csv"\nsingle_file = true')
	project = make_project(tmp_path, toml, "shop.table(description='Test table.')(rows)\n")
	give(project, "rows", [{"id": 1, "v": "a"}])
	load(project)
	give(project, "rows", [{"id": n, "v": "b"} for n in range(1001)] + [{"id": 1001, "v": "b", "extra": True}])
	done = sqlload(project, "load")
	assert done.returncode == 1
	assert "new columns appeared partway through the load, and single_file writes each file once" in done.stdout + done.stderr, done.stdout + done.stderr
	assert (project / "lake" / "rows" / "rows.csv").read_text().splitlines()[1].startswith("1,a,")


def test_given_a_files_target_with_day_partitions_when_delete_insert_reloads_a_day_then_it_is_replaced(tmp_path: Path) -> None:
	toml = FILES_PROJECT.replace('path = "{table}/"', 'path = "{table}/day={cursor:date}/"')
	project = make_project(tmp_path, toml, "shop.table(description='Test table.', write_strategy='delete_insert', cursor_column='day', columns={'day': 'date'})(rows)\n")
	give(project, "rows", [{"id": 1, "v": "a", "day": "2026-01-01"}, {"id": 2, "v": "b", "day": "2026-01-02"}])
	load(project)
	give(project, "rows", [{"id": 1, "v": "stale", "day": "2026-01-01"}, {"id": 2, "v": "B", "day": "2026-01-02"}, {"id": 3, "v": "c", "day": "2026-01-03"}])
	load(project)
	rows = ds.dataset(project / "lake" / "rows", format="parquet").to_table().to_pylist()
	assert sorted((row["id"], row["v"]) for row in rows) == [(1, "a"), (2, "B"), (3, "c")]


def test_given_masked_and_excluded_columns_when_loading_then_values_are_hashed_or_dropped(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', mask={'email': 'hash'}, exclude_columns=['secret*'])(rows)\n")
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


shop.table(description='Test table.', write_strategy='append'{settings})(priced)
"""


@pytest.mark.parametrize(
	("settings", "first", "amount", "expected_code", "expected_line", "expected_rows"),
	[
		(
			"",
			1,
			"2.50",
			1,
			"failed   RAW.shop__priced  the yielded data does not fit the table: amount is int in the table but decimal(21,2) in the source; pin the wider type to widen it, for example columns = { amount = \"decimal(21,2)\" }, or set auto_widen = true",
			[(1, 1)],
		),
		(
			", auto_widen=True",
			1,
			"2.50",
			0,
			"altered  RAW.shop__priced  widen  amount int -> decimal(21,2) (DECIMAL(21,2))  in place",
			[(1, Decimal("1.00")), (2, Decimal("2.50"))],
		),
		(
			", auto_widen=True",
			"10",
			"10.5",
			0,
			"altered  RAW.shop__priced  widen  amount decimal(19,0) -> decimal(20,1) (DECIMAL(20,1))  in place",
			[(1, Decimal("10.0")), (2, Decimal("10.5"))],
		),
		(
			", auto_widen=True",
			1,
			2.5,
			1,
			"failed   RAW.shop__priced  the yielded data does not fit the table: amount is int in the table but float in the source",
			[(1, 1)],
		),
	],
	ids=["fails-without-auto-widen", "auto-widen-to-decimal", "auto-widen-decimal-scale", "never-to-float"],
)
def test_given_an_inferred_column_that_outgrows_the_table_when_loading_then_auto_widen_decides(
	tmp_path: Path, settings: str, first: Any, amount: Any, expected_code: int, expected_line: str, expected_rows: list[tuple[Any, ...]]
) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, PRICED_TABLE.replace("{settings}", settings))
	give(project, "priced", [{"id": 1, "amount": first}])
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


@shop.table(description="Test table.", name="a")
def a(ctx):
    yield {"url": ctx.client.base_url}


@shop.table(description="Test table.", name="b")
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
		tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', write_strategy='merge', unique_key='seq', cursor_column='updated_at', lookback='1h')(rows)\n"
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
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', write_strategy='append')(rows)\n")
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
	project = make_project(tmp_path, DUCKDB_PROJECT, f"shop.table(description='Test table.', {settings})(rows)\n")
	applied = sqlload(project, "apply")
	assert applied.returncode == 0, applied.stderr
	assert query(project, "SELECT count(*) FROM information_schema.tables WHERE table_name = 'shop__rows'") == [(0,)]
	give(project, "rows", [{"id": 1, "total": 2.5, "at": "2026-01-01T00:00:00Z"}])
	done = load(project)
	assert "created  RAW.shop__rows  5 columns" in done.stdout
	assert query(project, 'SELECT id, total, epoch("at") FROM "RAW"."shop__rows"') == [(1, Decimal("2.50"), 1767225600.0)]


def test_given_new_fields_when_a_python_table_loads_then_the_changes_are_listed(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', write_strategy='append')(rows)\n")
	give(project, "rows", [{"id": 1}])
	load(project)
	give(project, "rows", [{"id": 2, "extra": "x"}])
	done = load(project)
	assert "altered  RAW.shop__rows  add column  extra text -> VARCHAR" in done.stdout, done.stdout


def test_given_a_pin_added_to_an_existing_table_when_planning_then_the_add_column_line_notes_what_its_type_loses(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', write_strategy='append')(rows)\n")
	give(project, "rows", [{"id": 1}])
	load(project)
	(project / "sources" / "shop.py").write_text(SHOP_MODULE.replace("{tables}", "shop.table(description='Test table.', write_strategy='append', columns={'code': 'varchar(8)'})(rows)\n"))
	give(project, "rows", [{"id": 2, "code": "x"}])
	planned = sqlload(project, "plan")
	assert planned.returncode == 0, planned.stderr
	assert "add column  code varchar(8) -> VARCHAR  (the length limit is not enforced)" in planned.stdout, planned.stdout


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
	project = make_project(tmp_path, toml, f"shop.table(description='Test table.', {settings})(rows)\n")
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
	project = make_project(tmp_path, DUCKDB_PROJECT, f"shop.table(description='Test table.', {settings})(rows)\n")
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
	project = make_project(tmp_path, toml, "shop.table(description='Test table.', write_strategy='delete_insert', cursor_column='day')(rows)\n")
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
	project = make_project(tmp_path, DUCKDB_PROJECT, f"shop.table(description='Test table.', write_strategy='append', cursor_column='v', columns={{'v': '{column_type}'}})(rows)\n")
	give(project, "rows", [{"v": value} for value in first])
	load(project)
	give(project, "rows", [{"v": value} for value in second])
	load(project)
	assert [value for (value,) in query(project, 'SELECT v FROM "RAW"."shop__rows" ORDER BY v')] == expected


@pytest.mark.parametrize(
	("column_type", "value", "message"),
	[
		("decimal(10,2)", 1.239, "amount: holds 1.239, which decimal(10,2) can't hold without losing digits"),
		("decimal(10,2)", "1.239", "amount: holds 1.239, which decimal(10,2) can't hold without losing digits"),
		("int", 2.5, "amount: holds 2.5, which int64 can't hold without losing digits"),
	],
	ids=["float-to-decimal", "text-to-decimal", "float-to-int"],
)
def test_given_a_pinned_narrower_type_when_a_value_would_lose_digits_then_the_load_fails_and_nothing_loads(
	tmp_path: Path, column_type: str, value: Any, message: str
) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, f"shop.table(description='Test table.', columns={{'amount': '{column_type}'}})(rows)\n")
	give(project, "rows", [{"id": 1, "amount": 1}, {"id": 2, "amount": value}])
	done = sqlload(project, "load")
	assert done.returncode == 1, done.stdout + done.stderr
	assert message in done.stdout + done.stderr, done.stdout + done.stderr
	assert query(project, 'SELECT count(*) FROM "RAW"."shop__rows"') == [(0,)]


def test_given_cursor_start_when_the_cursor_type_is_not_known_yet_then_ctx_gets_the_same_type_on_every_run(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', write_strategy='append', cursor_column='at', cursor_start='2026-01-01T00:00:00Z')(rows)\n")
	give(project, "rows", [])
	load(project)
	assert seen(project, "rows")["cursor_value"] == "datetime.datetime(2026, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)"
	give(project, "rows", [{"id": 1, "at": "2026-01-05T00:00:00Z"}])
	load(project)
	assert seen(project, "rows")["cursor_value"] == "datetime.datetime(2026, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)"
	load(project)
	assert seen(project, "rows")["cursor_value"] == "datetime.datetime(2026, 1, 5, 0, 0, tzinfo=datetime.timezone.utc)"


def test_given_a_target_connection_when_a_table_function_asks_for_it_then_it_is_withheld(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.')(rows)\n")
	give(project, "rows", [{"id": 1}])
	load(project)
	assert seen(project, "rows")["other"]["base_url"] == "https://api.shop.example"
	assert seen(project, "rows")["target"].startswith("PermissionError: connection wh is used by a [targets] entry")


NAMED_MODULE = """
import sqlload as sl

shop = sl.source("shop", table_name="api_{table}")


@shop.table(description="Test table.", name="orders")
def orders(ctx):
    yield {"id": 1}


@shop.table(description="Test table.", name="stores", table_name="{source}_stores_v2")
def stores(ctx):
    yield {"id": 1}


@sl.table
def standalone(ctx):
    '''A table described by its docstring.'''
    yield {"id": 1}
"""


def test_given_table_name_settings_when_loading_then_tables_are_named_from_them(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "", module=NAMED_MODULE)
	load(project)
	tables = query(project, "SELECT table_name FROM information_schema.tables ORDER BY table_name")
	assert tables == [("api_orders",), ("shop_stores_v2",), ("standalone",)]
	comments = query(project, "SELECT table_name, comment FROM duckdb_tables() ORDER BY table_name")
	assert comments == [("api_orders", "Test table."), ("shop_stores_v2", "Test table."), ("standalone", "A table described by its docstring.")]


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
	project = make_project(tmp_path, DUCKDB_PROJECT, f"shop.table(description='Test table.', {settings})(rows)\n")
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
	project = make_project(tmp_path, FILES_PROJECT, f"shop.table(description='Test table.', {settings})(rows)\n")
	give(project, "rows", [{"id": 1, "v": "a"}])
	load(project)
	give(project, "rows", [])
	done = sqlload(project, "load")
	assert (done.returncode == 1) == fails, done.stdout + done.stderr
	assert ("yielded no rows; replace would empty " in done.stdout + done.stderr) == fails, done.stderr
	assert ("lake/rows/; set allow_empty=True" in done.stdout + done.stderr) == fails, done.stderr
	assert files_rows(project, "rows") == expected_rows


def test_given_allow_empty_on_a_source_with_a_bad_value_when_planning_then_the_setting_is_validated(tmp_path: Path) -> None:
	module = "import sqlload as sl\nshop = sl.source('shop', allow_empty='yes')\nshop.table(description='Test table.', name='t')(lambda ctx: [])\n"
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
	module = "import sqlload as sl\nshop = sl.source('shop', connection='wh')\nshop.table(description='Test table.', name='t')(lambda ctx: [])\n"
	project = make_project(tmp_path, DUCKDB_PROJECT, "", module=module)
	done = sqlload(project, "plan")
	assert done.returncode == 1
	assert "Python source shop uses connection wh, which target dev loads into; Python code never gets warehouse credentials" in done.stdout + done.stderr, done.stderr


LAKE_MODULE = """
import sqlload as sl

lake = sl.source("lake_files", connection="lake")


@lake.table(description="Test table.", name="listing")
def listing(ctx):
    yield {"root": ctx.connection["root"]}
"""


def test_given_a_python_source_on_a_files_targets_connection_when_loading_then_it_reads_that_connection(tmp_path: Path) -> None:
	project = make_project(tmp_path, FILES_PROJECT, "", module=LAKE_MODULE)
	load(project)
	rows = ds.dataset(project / "lake" / "listing", format="parquet").to_table().to_pylist()
	assert [row["root"] for row in rows] == ["lake"]


def test_given_a_loaded_python_table_when_generating_its_contract_then_it_prints_the_columns_to_paste_and_writes_nothing(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', columns={'code': 'text'})(rows)\n")
	give(project, "rows", [{"id": 1, "code": "x1", "ok": True}])
	load(project)
	module = (project / "sources" / "shop.py").read_text()
	toml = (project / "sqlload.toml").read_text()
	done = sqlload(project, "contract", "generate", "--write")
	assert done.returncode == 0, done.stdout + done.stderr
	assert done.stdout.splitlines() == [
		"Contract comparison",
		"  OK    table:shop__rows",
		"        adopt: paste into the table's declaration in sources/shop.py",
		'          columns={"id": "int", "code": "text", "ok": "bool"}',
		"",
		"0 contract difference(s)",
	], done.stdout
	assert ((project / "sources" / "shop.py").read_text(), (project / "sqlload.toml").read_text()) == (module, toml)


def test_given_a_python_table_when_diffing_with_source_then_it_is_inspection_unavailable_since_its_columns_are_known_only_when_it_runs(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', columns={'code': 'text'})(rows)\n")
	give(project, "rows", [{"id": 1, "code": "x1"}])
	load(project)
	done = sqlload(project, "contract", "diff", "--source", "--json")
	findings = json.loads(done.stdout)["resources"][0]["findings"]
	assert (done.returncode, [finding["kind"] for finding in findings], "known only when it runs" in findings[0]["message"]) == (1, ["inspection_unavailable"], True), done.stdout + done.stderr


def test_given_a_files_target_with_an_older_file_of_other_columns_when_diffing_all_files_then_the_drift_is_reported(tmp_path: Path) -> None:
	import pyarrow as pa
	import pyarrow.parquet as pq

	project = make_project(tmp_path, FILES_PROJECT, "shop.table(description='Test table.', columns={'code': 'text'})(rows)\n")
	give(project, "rows", [{"id": 1, "code": "x1"}])
	load(project)
	agreed = sqlload(project, "contract", "diff", "--all-files")
	assert (agreed.returncode, agreed.stdout.splitlines()[1]) == (0, "  OK    table:shop__rows"), agreed.stdout + agreed.stderr
	old = project / "lake" / "rows" / "old.parquet"
	pq.write_table(pa.table({"id": pa.array([0], pa.int64()), "code": pa.array([7], pa.int64()), "gone": [True]}), old)
	os.utime(old, (1_000_000, 1_000_000))
	assert sqlload(project, "contract", "diff").returncode == 0
	drift = sqlload(project, "contract", "diff", "--all-files", "--json")
	assert drift.returncode == 1, drift.stdout + drift.stderr
	assert [(finding["kind"], finding["column"]) for finding in json.loads(drift.stdout)["resources"][0]["findings"]] == [
		("files_type_mismatch", "code"),
		("files_extra_column", "gone"),
	]


def test_given_a_partitioned_files_target_when_diffing_then_the_newest_partition_is_the_baseline_and_a_broken_file_is_one_finding(tmp_path: Path) -> None:
	import pyarrow as pa
	import pyarrow.parquet as pq

	toml = FILES_PROJECT.replace('path = "{table}/"', 'path = "{table}/day={cursor:date}/"\nmax_footer_reads = 1')
	project = make_project(tmp_path, toml, "shop.table(description='Test table.', write_strategy='delete_insert', cursor_column='day', columns={'day': 'date', 'v': 'text'})(rows)\n")
	give(project, "rows", [{"id": 1, "v": "a", "day": "2026-01-01"}, {"id": 2, "v": "b", "day": "2026-01-02"}])
	load(project)
	agreed = sqlload(project, "contract", "diff")
	assert (agreed.returncode, agreed.stdout.splitlines()[1]) == (0, "  OK    table:shop__rows"), agreed.stdout + agreed.stderr
	old = project / "lake" / "rows" / "day=2025-12-31"
	old.mkdir()
	pq.write_table(pa.table({"id": pa.array([0], pa.int64()), "v": pa.array([7], pa.int64())}), old / "old.parquet")
	drift = sqlload(project, "contract", "diff", "--all-files", "--json")
	assert [(finding["kind"], finding["column"]) for finding in json.loads(drift.stdout)["resources"][0]["findings"]] == [
		("files_type_mismatch", "v"),
		("files_missing_column", "day"),
	], drift.stdout + drift.stderr
	(old / "broken.parquet").write_text("not parquet")
	broken = sqlload(project, "contract", "diff", "--all-files", "--json")
	assert (broken.returncode, [finding["kind"] for finding in json.loads(broken.stdout)["resources"][0]["findings"]]) == (1, ["inspection_unavailable"]), broken.stdout + broken.stderr


def test_given_a_cursor_left_undeclared_after_a_load_when_diffing_a_partitioned_files_target_then_it_reads_partitions_as_plan_does(tmp_path: Path) -> None:
	toml = FILES_PROJECT.replace('path = "{table}/"', 'path = "{table}/day={cursor:date}/"')
	project = make_project(tmp_path, toml, "shop.table(description='Test table.', write_strategy='delete_insert', cursor_column='day', columns={'day': 'date', 'v': 'text'})(rows)\n")
	give(project, "rows", [{"id": 1, "v": "a", "day": "2026-01-01"}])
	load(project)
	shop = project / "sources" / "shop.py"
	shop.write_text(shop.read_text().replace("'day': 'date', ", ""))
	(project / "lake" / "rows" / "stray.parquet").write_bytes(next((project / "lake" / "rows").glob("day=*/*.parquet")).read_bytes())
	diff = sqlload(project, "contract", "diff")
	assert (diff.returncode, "stray.parquet is not in a partition folder" in diff.stdout) == (1, True), diff.stdout + diff.stderr


def test_given_an_enforced_contract_without_declared_columns_when_planning_then_the_table_is_blocked(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', contract='enforced')(rows)\n")
	done = sqlload(project, "plan")
	assert done.returncode == 1
	assert "contract enforced needs declared columns: the first load would create a table with only metadata columns" in done.stdout + done.stderr


@pytest.mark.parametrize(
	("first", "second", "expected_code", "expected_text", "expected_columns"),
	[
		(
			[{"order_id": 7, "customer": {"country": "GB", "address": {"city": "Leeds"}}, "line_totals": {"sku-14": 12.5}}],
			[{"order_id": 8, "customer": {"country": "FR", "tier": "gold"}, "line_totals": {"sku-17": 1}}],
			0,
			"loaded   RAW.shop__rows  1 rows",
			[
				("order_id", "BIGINT"),
				("customer__country", "VARCHAR"),
				("customer__address", "JSON"),
				("line_totals", "JSON"),
				("_sqlload_load_id", "VARCHAR"),
				("_sqlload_loaded_at", "TIMESTAMP WITH TIME ZONE"),
				("customer__tier", "VARCHAR"),
			],
		),
		(
			[{"order_id": 7, "customer": {"country": "GB"}}],
			[{"order_id": 8, "customer": "GB"}],
			1,
			"flatten customer: row 1 of a batch holds a text value, not an object",
			[("order_id", "BIGINT"), ("customer__country", "VARCHAR"), ("_sqlload_load_id", "VARCHAR"), ("_sqlload_loaded_at", "TIMESTAMP WITH TIME ZONE")],
		),
	],
	ids=["one-level-and-new-keys", "shape-change-fails"],
)
def test_given_flatten_when_loading_then_only_the_listed_object_expands_one_level(
	tmp_path: Path, first: Any, second: Any, expected_code: int, expected_text: str, expected_columns: list[tuple[str, str]]
) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', write_strategy='append', flatten=['customer'])(rows)\n")
	give(project, "rows", first)
	planned = sqlload(project, "plan")
	assert "columns inferred on first load, customer flattened one level" in planned.stdout, planned.stdout
	load(project)
	give(project, "rows", second)
	done = sqlload(project, "load")
	assert done.returncode == expected_code, done.stdout + done.stderr
	assert expected_text in done.stdout, done.stdout + done.stderr
	columns = query(project, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'shop__rows' ORDER BY ordinal_position")
	assert columns == expected_columns


FLATTEN_YIELDS = {
	"dicts": "[{'id': 1, 'customer': {'country': 'GB', 'address': {'city': 'Leeds'}}}, {'id': 2, 'customer': None}]",
	"bytes": "sl.json(__import__('json').dumps([{'id': 1, 'customer': {'country': 'GB', 'address': {'city': 'Leeds'}}}, {'id': 2, 'customer': None}]).encode())",
	"arrow": "__import__('pyarrow').table({'id': [1, 2], 'customer': [{'country': 'GB', 'address': {'city': 'Leeds'}}, None]})",
}


@pytest.mark.parametrize("test_case", ["dicts", "bytes", "arrow"])
def test_given_flatten_when_yielding_dicts_bytes_or_arrow_then_the_tables_match(tmp_path: Path, test_case: str) -> None:
	module = f"import sqlload as sl\nshop = sl.source('shop')\nshop.table(description='Test table.', name='t', flatten=['customer'])(lambda ctx: [{FLATTEN_YIELDS[test_case]}])\n"
	project = make_project(tmp_path, DUCKDB_PROJECT, "", module=module)
	load(project)
	columns = query(project, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'shop__t' ORDER BY ordinal_position")
	assert columns[:3] == [("id", "BIGINT"), ("customer__country", "VARCHAR"), ("customer__address", "JSON")]
	assert query(project, 'SELECT id, customer__country, customer__address::VARCHAR FROM "RAW".shop__t ORDER BY id') == [(1, "GB", '{"city":"Leeds"}'), (2, None, None)]


def test_given_flatten_when_a_key_collides_with_a_field_then_the_load_fails(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', flatten=['customer'])(rows)\n")
	give(project, "rows", [{"id": 1, "customer": {"country": "GB"}, "customer__country": "x"}])
	done = sqlload(project, "load")
	assert done.returncode == 1
	assert "flatten customer: the key country gives column customer__country, which the data already has" in done.stdout, done.stdout + done.stderr



TYPED_FLATTEN = {
	"dicts": "[{'id': 1, 'totals': {'net': __import__('decimal').Decimal('12.50'), 'day': __import__('datetime').date(2024, 1, 2), 'at': __import__('datetime').datetime(2024, 1, 2, 3, 4, 5)}}]",
	"arrow": "__import__('pyarrow').table({'id': [1], 'totals': __import__('pyarrow').array([{'net': __import__('decimal').Decimal('12.50'), 'day': __import__('datetime').date(2024, 1, 2), 'at': __import__('datetime').datetime(2024, 1, 2, 3, 4, 5)}], __import__('pyarrow').struct([('net', __import__('pyarrow').decimal128(4, 2)), ('day', __import__('pyarrow').date32()), ('at', __import__('pyarrow').timestamp('us'))]))})",
}


@pytest.mark.parametrize("test_case", ["dicts", "arrow"])
def test_given_flatten_when_the_object_holds_typed_values_then_its_columns_keep_their_types(tmp_path: Path, test_case: str) -> None:
	module = f"import sqlload as sl\nshop = sl.source('shop')\nshop.table(description='Test table.', name='t', flatten=['totals'])(lambda ctx: [{TYPED_FLATTEN[test_case]}])\n"
	project = make_project(tmp_path, DUCKDB_PROJECT, "", module=module)
	load(project)
	columns = query(project, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'shop__t' ORDER BY ordinal_position")
	assert [(name, kind.split("(")[0]) for name, kind in columns[:4]] == [("id", "BIGINT"), ("totals__net", "DECIMAL"), ("totals__day", "DATE"), ("totals__at", "TIMESTAMP")]
	assert query(project, 'SELECT totals__net::VARCHAR, totals__day::VARCHAR, totals__at::VARCHAR FROM "RAW".shop__t') == [("12.50", "2024-01-02", "2024-01-02 03:04:05")]


def test_given_flatten_when_a_later_batch_has_a_field_of_a_flattened_name_then_the_load_fails(tmp_path: Path) -> None:
	module = "import sqlload as sl\nshop = sl.source('shop')\nshop.table(description='Test table.', name='t', flatten=['customer'])(lambda ctx: [[{'id': 1, 'customer': {'country': 'GB'}}], sl.json(b'[{\"id\": 2, \"customer__country\": \"x\"}]')])\n"
	project = make_project(tmp_path, DUCKDB_PROJECT, "", module=module)
	done = sqlload(project, "load")
	assert done.returncode == 1
	assert "flatten: column customer__country comes both from a field of the rows and from flattening a listed object" in done.stdout, done.stdout + done.stderr


@pytest.mark.parametrize(
	("settings", "expected_problem"),
	[
		("exclude_columns=['customer']", "flatten customer: exclude_columns customer also names customer, which flattening replaces with customer__<key> columns"),
		("mask={'customer': 'hash'}", "flatten customer: mask also names customer"),
		("columns={'customer': 'json'}", "flatten customer: columns also names customer"),
	],
	ids=["exclude", "mask", "pin"],
)
def test_given_flatten_and_a_setting_on_the_same_object_when_planning_then_it_is_refused(tmp_path: Path, settings: str, expected_problem: str) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, f"shop.table(description='Test table.', flatten=['customer'], {settings})(rows)\n")
	give(project, "rows", [{"id": 1, "customer": {"country": "GB"}}])
	done = sqlload(project, "plan")
	assert done.returncode == 1
	assert expected_problem in done.stdout, done.stdout + done.stderr



PA = "__import__('pyarrow')"

FLATTEN_ARROW_CASES = {
	"masked-struct-required-child": f"{PA}.table({{'id': [1, 2], 'customer': {PA}.StructArray.from_arrays([{PA}.array(['GB', 'FR'])], fields=[{PA}.field('country', {PA}.string(), nullable=False)], mask={PA}.array([False, True]))}})",
	"map": f"{PA}.table({{'id': [1, 2], 'customer': {PA}.array([[('country', 'GB')], None], {PA}.map_({PA}.string(), {PA}.string()))}})",
	"arrow-then-dicts": f"{PA}.table({{'id': [1], 'customer': [{{'country': 'GB'}}]}}), [{{'id': 2, 'customer': {{'country': None}}}}]",
}


@pytest.mark.parametrize("test_case", list(FLATTEN_ARROW_CASES))
def test_given_flatten_on_arrow_structs_and_maps_when_loading_then_null_parents_give_null_keys(tmp_path: Path, test_case: str) -> None:
	module = f"import sqlload as sl\nshop = sl.source('shop')\nshop.table(description='Test table.', name='t', flatten=['customer'])(lambda ctx: [{FLATTEN_ARROW_CASES[test_case]}])\n"
	project = make_project(tmp_path, DUCKDB_PROJECT, "", module=module)
	load(project)
	assert query(project, 'SELECT id, customer__country FROM "RAW".shop__t ORDER BY id') == [(1, "GB"), (2, None)]


def test_given_flatten_when_walked_rows_collide_then_the_load_fails(tmp_path: Path) -> None:
	module = "import decimal\nimport sqlload as sl\nshop = sl.source('shop')\nshop.table(description='Test table.', name='t', flatten=['customer'])(lambda ctx: [[{'n': decimal.Decimal('1.5'), 'customer': {'country': 'GB'}}, {'n': decimal.Decimal('2'), 'customer__country': 'x'}]])\n"
	project = make_project(tmp_path, DUCKDB_PROJECT, "", module=module)
	done = sqlload(project, "load")
	assert done.returncode == 1
	assert "flatten customer: the key country gives column customer__country, which the data already has" in done.stdout, done.stdout + done.stderr


def test_given_an_exclude_glob_covering_the_key_columns_when_loading_then_they_stay_excluded(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', flatten=['customer'], exclude_columns=['cust*'])(rows)\n")
	give(project, "rows", [{"id": 1, "customer": {"country": "GB"}}])
	load(project)
	columns = query(project, "SELECT column_name FROM information_schema.columns WHERE table_name = 'shop__rows' ORDER BY ordinal_position")
	assert columns == [("id",), ("_sqlload_load_id",), ("_sqlload_loaded_at",)]


def test_given_flatten_with_a_path_when_planning_then_it_is_refused(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', flatten=['customer.address'])(rows)\n")
	give(project, "rows", [{"id": 1}])
	done = sqlload(project, "plan")
	assert done.returncode == 1
	assert "flatten customer.address: flatten names top-level objects, not paths; list customer instead" in done.stdout, done.stdout + done.stderr


def test_given_flatten_on_a_files_source_when_planning_then_it_is_refused(tmp_path: Path) -> None:
	toml = DUCKDB_PROJECT + '\n[connections.lake]\nkind = "filesystem"\nroot = "lake"\n\n[sources.events]\nkind = "files"\nconnection = "lake"\npath = "events/"\nformat = "ndjson"\ndescription = "Events."\n[sources.events.tables.events]\nflatten = ["customer"]\n'
	project = make_project(tmp_path, toml, "")
	(project / "lake" / "events").mkdir()
	(project / "lake" / "events" / "a.ndjson").write_text('{"customer": {"country": "GB"}}\n')
	done = sqlload(project, "plan")
	assert done.returncode == 1
	assert "flatten applies to Python tables; nested values from this source stay JSON" in done.stdout, done.stdout + done.stderr


DATE_CURSOR_MODULE = '''
import datetime

import sqlload as sl

shop = sl.source("shop", connection="shop")


@shop.table(description="Test table.", write_strategy="append", cursor_column="day")
def days(ctx):
    yield {"id": 1, "day": datetime.date(2026, 11, 1)}
'''


def test_given_append_with_an_undeclared_date_cursor_when_loading_then_it_is_refused_before_the_table_exists(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "", module=DATE_CURSOR_MODULE)
	done = sqlload(project, "load")
	assert (done.returncode, 'cannot follow the date cursor day' in done.stdout + done.stderr) == (1, True), done.stdout + done.stderr
	assert query(project, "SELECT count(*) FROM information_schema.tables WHERE table_name = 'shop__days'") == [(0,)]


@pytest.mark.parametrize(
	"data",
	[
		"__import__('pyarrow').table({'id': [1, 2, 3], 'xs': __import__('pyarrow').array([[float('nan'), float('inf'), -float('inf'), 1.5], None, [None]], __import__('pyarrow').list_(__import__('pyarrow').float32()))})",
		"__import__('polars').DataFrame({'id': [1, 2, 3], 'xs': [[float('nan'), float('inf'), -float('inf'), 1.5], None, [None]]})",
	],
	ids=["pyarrow", "polars"],
)
def test_given_non_finite_floats_in_an_arrow_list_when_loading_then_a_pinned_array_keeps_them_and_json_is_unchanged(tmp_path: Path, data: str) -> None:
	module = (
		"import sqlload as sl\nshop = sl.source('shop')\n"
		f"shop.table(description='Test table.', name='pinned', columns={{'xs': 'array(float)'}})(lambda ctx: [{data}])\n"
		f"shop.table(description='Test table.', name='inferred')(lambda ctx: [{data}])\n"
	)
	project = make_project(tmp_path, DUCKDB_PROJECT, "", module=module)
	load(project)
	assert query(project, 'SELECT id, xs::VARCHAR FROM "RAW".shop__pinned ORDER BY id') == [(1, "[nan, inf, -inf, 1.5]"), (2, None), (3, "[NULL]")]
	assert query(project, 'SELECT id, xs::VARCHAR FROM "RAW".shop__inferred ORDER BY id') == [(1, "[null,null,null,1.5]"), (2, None), (3, "[null]")]


def test_given_an_int_array_pinned_over_arrow_float_lists_of_whole_numbers_when_loading_then_they_load_without_a_note(tmp_path: Path) -> None:
	data = "__import__('pyarrow').table({'id': [1], 'xs': [[1.0, 2.0]]})"
	module = f"import sqlload as sl\nshop = sl.source('shop')\nshop.table(description='Test table.', name='t', columns={{'xs': 'array(int)'}})(lambda ctx: [{data}])\n"
	project = make_project(tmp_path, DUCKDB_PROJECT, "", module=module)
	planned = sqlload(project, "plan")
	assert planned.returncode == 0, planned.stderr
	assert "xs array(int)   -> BIGINT[]\n" in planned.stdout, planned.stdout
	load(project)
	assert query(project, 'SELECT xs::VARCHAR FROM "RAW".shop__t') == [("[1, 2]",)]


@pytest.mark.parametrize(
	("data", "column_type", "message"),
	[
		("__import__('pyarrow').table({'id': [1], 'xs': [[2.0, 1.5]]})", "array(int)", "an element holds 1.5, which int64 can't hold without losing digits"),
		("__import__('pyarrow').table({'id': [1], 'xs': [[1.25, 1.239]]})", "array(decimal(10,2))", "an element holds 1.239, which decimal(10,2) can't hold without losing digits"),
		("{'id': 1, 'xs': [1.25, 1.239]}", "array(decimal(10,2))", "holds 1.239, which decimal(10,2) can't hold without losing digits"),
	],
	ids=["arrow-floats-into-int", "arrow-floats-into-decimal", "dict-list-into-decimal"],
)
def test_given_an_array_pin_when_an_element_would_lose_digits_then_the_load_fails_and_nothing_loads(
	tmp_path: Path, data: str, column_type: str, message: str
) -> None:
	module = f"import sqlload as sl\nshop = sl.source('shop')\nshop.table(description='Test table.', name='t', columns={{'xs': '{column_type}'}})(lambda ctx: [{data}])\n"
	project = make_project(tmp_path, DUCKDB_PROJECT, "", module=module)
	done = sqlload(project, "load")
	assert done.returncode == 1, done.stdout + done.stderr
	assert message in done.stdout + done.stderr, done.stdout + done.stderr
	assert query(project, 'SELECT count(*) FROM "RAW".shop__t') == [(0,)]


@pytest.mark.parametrize(
	("data", "column_type", "expected"),
	[
		(
			"[__import__('pyarrow').table({'id': [1], 'xs': [[]]}), __import__('pyarrow').table({'id': [2], 'xs': [[float('nan'), 1]]}), __import__('pyarrow').table({'id': [3], 'xs': [[float('inf'), 2.5]]})]",
			"array(float)",
			[(1, "[]"), (2, "[nan, 1.0]"), (3, "[inf, 2.5]")],
		),
		(
			"[__import__('pyarrow').table({'id': [1], 'xs': __import__('pyarrow').array([[18446744073709551615, 1]], __import__('pyarrow').list_(__import__('pyarrow').uint64()))})]",
			"array(decimal(20,0))",
			[(1, "[18446744073709551615, 1]")],
		),
		(
			"[__import__('pyarrow').table({'id': [1, 2], 'xs': [[], None]})]",
			"array(float)",
			[(1, "[]"), (2, None)],
		),
	],
	ids=["empty list, then int, then float batches", "uint64 beyond int64", "only empty and NULL lists"],
)
def test_given_arrow_list_batches_when_loading_a_pinned_array_then_every_batch_keeps_its_values(
	tmp_path: Path, data: str, column_type: str, expected: list[tuple[int, str]]
) -> None:
	module = f"import sqlload as sl\nshop = sl.source('shop')\nshop.table(description='Test table.', name='t', columns={{'xs': '{column_type}'}})(lambda ctx: {data})\n"
	project = make_project(tmp_path, DUCKDB_PROJECT, "", module=module)
	load(project)
	assert query(project, 'SELECT id, xs::VARCHAR FROM "RAW".shop__t ORDER BY id') == expected


def test_given_dlt_naming_when_a_source_spelled_arrow_list_is_pinned_as_an_array_then_it_crosses_as_a_list(tmp_path: Path) -> None:
	data = "[__import__('pyarrow').table({'ID': [1], 'Scores': [[float('nan'), 1]]})]"
	module = f"import sqlload as sl\nshop = sl.source('shop')\nshop.table(description='Test table.', name='t', naming='dlt', columns={{'Scores': 'array(float)'}})(lambda ctx: {data})\n"
	project = make_project(tmp_path, DUCKDB_PROJECT, "", module=module)
	load(project)
	assert query(project, 'SELECT id, scores::VARCHAR FROM "RAW".shop__t') == [(1, "[nan, 1.0]")]


def test_given_a_native_type_pinned_for_a_files_target_when_planning_then_plan_refuses_the_column(tmp_path: Path) -> None:
	module = "import sqlload as sl\nshop = sl.source('shop')\nshop.table(description='Test table.', name='t', columns={'n': 'native(\"HUGEINT\")'})(lambda ctx: [{'id': 1, 'n': 5}])\n"
	project = make_project(tmp_path, FILES_PROJECT, "", module=module)
	planned = sqlload(project, "plan")
	assert planned.returncode == 1, planned.stdout
	assert "column n: a native type has no Arrow type, so Parquet and CSV files cannot hold it; pin a portable type" in planned.stdout + planned.stderr, planned.stdout + planned.stderr


#: Two tenants' tables of one API merged into one table, told apart by their constant `tenant_id`.
TENANT_TABLES = """\
for tenant in ("a", "b"):
    shop.table(
        description="Test table.",
        name=f"orders_{tenant}",
        table_name="orders",
        constant_columns={"tenant_id": tenant},
        write_strategy="merge",
        unique_key=["tenant_id", "id"],
        cursor_column="at",
    )(rows)
"""

TENANT_ROWS = 'SELECT tenant_id, id, v, _sqlload_load_id, CAST(_sqlload_loaded_at AS VARCHAR) FROM "RAW"."orders" WHERE tenant_id = ? ORDER BY id'


def test_given_two_tenants_with_constant_columns_when_merging_into_one_table_then_each_keeps_its_own_rows_and_cursor(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, TENANT_TABLES)
	give(project, "orders_a", [{"id": 1, "v": "a1", "at": "2026-01-05T00:00:00Z"}, {"id": 2, "v": "a2", "at": "2026-01-06T00:00:00Z"}])
	give(project, "orders_b", [{"id": 1, "v": "b1", "at": "2026-01-01T00:00:00Z"}])
	load(project)
	assert query(project, 'SELECT tenant_id, id, v FROM "RAW"."orders" ORDER BY tenant_id, id') == [("a", 1, "a1"), ("a", 2, "a2"), ("b", 1, "b1")]
	tenant_a = query(project, TENANT_ROWS.replace("?", "'a'"))
	give(project, "orders_b", [{"id": 1, "v": "b1-changed", "at": "2026-01-02T00:00:00Z"}])
	load(project, "orders_b")
	assert seen(project, "orders_b")["cursor_value"] == "datetime.datetime(2026, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)"
	assert query(project, TENANT_ROWS.replace("?", "'a'")) == tenant_a
	assert query(project, 'SELECT tenant_id, id, v FROM "RAW"."orders" ORDER BY tenant_id, id') == [("a", 1, "a1"), ("a", 2, "a2"), ("b", 1, "b1-changed")]
	columns = query(project, "SELECT column_name FROM information_schema.columns WHERE table_name = 'orders' ORDER BY ordinal_position")
	assert [name for (name,) in columns] == ["id", "v", "at", "tenant_id", "_sqlload_load_id", "_sqlload_loaded_at"]


def test_given_a_table_yielding_its_constant_column_when_loading_then_it_fails_and_nothing_loads(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', constant_columns={'tenant_id': 'a'})(rows)\n")
	give(project, "rows", [{"id": 1, "tenant_id": "x"}])
	done = sqlload(project, "load")
	assert done.returncode == 1
	assert "constant column tenant_id: the source already has a column tenant_id, and a constant never replaces it" in done.stdout + done.stderr, done.stdout + done.stderr
	assert query(project, "SELECT count(*) FROM information_schema.tables WHERE table_name = 'shop__rows'") == [(0,)]


def test_given_a_python_table_repeating_a_merge_key_when_loading_then_dedup_sort_picks_the_row_and_no_row_number_lands(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', write_strategy='merge', unique_key='id', dedup_sort={'v': 'desc'})(rows)\n")
	give(project, "rows", [{"id": 1, "v": "b"}, {"id": 1, "v": "c"}, {"id": 1, "v": "a"}, {"id": 2, "v": "x"}])
	load(project)
	assert query(project, 'SELECT id, v FROM "RAW"."shop__rows" ORDER BY id') == [(1, "c"), (2, "x")]
	columns = query(project, "SELECT column_name FROM information_schema.columns WHERE table_name = 'shop__rows' ORDER BY ordinal_position")
	assert [name for (name,) in columns] == ["id", "v", "_sqlload_load_id", "_sqlload_loaded_at"]


def test_given_a_python_table_repeating_a_merge_key_when_it_must_fail_then_the_load_names_the_key_and_merges_nothing(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', write_strategy='merge', unique_key='id', on_duplicate_keys='fail')(rows)\n")
	give(project, "rows", [{"id": 1, "v": "b"}, {"id": 1, "v": "c"}, {"id": 2, "v": "x"}])
	done = sqlload(project, "load")
	assert done.returncode == 1, done.stdout + done.stderr
	assert "the loaded rows repeat its unique_key: 1 value(s) of (id) appear in more than one row, such as (1)" in done.stdout + done.stderr, done.stdout + done.stderr
	assert query(project, 'SELECT count(*) FROM "RAW"."shop__rows"') == [(0,)]


@pytest.mark.parametrize(
	("settings", "expected"),
	[
		("write_strategy='append', dedup_sort={'v': 'desc'}", 'dedup_sort is for write_strategy = "merge", which matches rows on unique_key; append keeps every row'),
		("write_strategy='merge', unique_key='id', dedup_sort={'v': 'desc', 'w': 'asc'}", 'dedup_sort names one column and its direction, such as { updated_at = "desc" }'),
		("write_strategy='merge', unique_key='id', dedup_sort={'id': 'desc'}", "dedup_sort column id is in the unique_key"),
		(
			"write_strategy='merge', unique_key='id', dedup_sort={'v': 'desc'}, on_duplicate_keys='fail'",
			'dedup_sort picks which repeated row a merge keeps, but on_duplicate_keys = "fail" keeps none',
		),
	],
	ids=["not_merge", "two_columns", "key_column", "with_fail"],
)
def test_given_a_python_table_with_a_dedupe_setting_it_cannot_use_when_planning_then_plan_refuses_it(tmp_path: Path, settings: str, expected: str) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT, f"shop.table(description='Test table.', {settings})(rows)\n")
	planned = sqlload(project, "plan")
	assert planned.returncode == 1, planned.stdout
	assert expected in planned.stdout + planned.stderr, planned.stdout + planned.stderr


def test_given_source_level_dedupe_settings_when_loading_then_only_its_merge_tables_take_them_and_a_table_can_choose_fail(tmp_path: Path) -> None:
	module = SHOP_MODULE.replace('sl.source("shop", connection="shop")', 'sl.source("shop", connection="shop", dedup_sort={"v": "desc"})')
	tables = (
		"shop.table(description='Test table.', name='log', write_strategy='append')(rows)\n"
		"shop.table(description='Test table.', name='kept', write_strategy='merge', unique_key='id')(rows)\n"
		"shop.table(description='Test table.', name='strict', write_strategy='merge', unique_key='id', on_duplicate_keys='fail')(rows)\n"
	)
	project = make_project(tmp_path, DUCKDB_PROJECT, tables, module=module)
	repeated = [{"id": 1, "v": "b"}, {"id": 1, "v": "c"}, {"id": 1, "v": "a"}]
	for table in ("log", "kept", "strict"):
		give(project, table, repeated)
	done = sqlload(project, "load")
	assert done.returncode == 1, done.stdout + done.stderr
	assert "the loaded rows repeat its unique_key" in done.stdout + done.stderr, done.stdout + done.stderr
	assert query(project, 'SELECT count(*) FROM "RAW"."shop__log"') == [(3,)]
	assert query(project, 'SELECT id, v FROM "RAW"."shop__kept"') == [(1, "c")]
	assert query(project, 'SELECT count(*) FROM "RAW"."shop__strict"') == [(0,)]


def test_given_a_source_that_fails_on_repeated_keys_when_a_table_sets_dedup_sort_then_that_table_keeps_its_ranked_row(tmp_path: Path) -> None:
	module = SHOP_MODULE.replace('sl.source("shop", connection="shop")', 'sl.source("shop", connection="shop", on_duplicate_keys="fail")')
	project = make_project(tmp_path, DUCKDB_PROJECT, "shop.table(description='Test table.', write_strategy='merge', unique_key='id', dedup_sort={'v': 'asc'})(rows)\n", module=module)
	give(project, "rows", [{"id": 1, "v": "b"}, {"id": 1, "v": "a"}, {"id": 1, "v": "c"}])
	load(project)
	assert query(project, 'SELECT id, v FROM "RAW"."shop__rows"') == [(1, "a")]
