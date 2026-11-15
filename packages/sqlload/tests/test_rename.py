"""`rename` end to end: an `@sl.rename` function picked by name in `sqlload.toml` or passed to a Python table, into DuckDB."""

import os
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import duckdb
import pytest

PROJECT = """\
python_paths = ["renames"]

[connections.wh]
kind = "duckdb"
database = "wh.duckdb"

[connections.files]
kind = "filesystem"
root = "."

[targets.dev]
connection = "wh"
loader_schema = "raw"

[sources.orders]
kind = "files"
connection = "files"
path = "drop/"
format = "csv"
description = "ERP exports."
write_strategy = "replace"
[sources.orders.tables.orders]
rename = "{rename}"
"""

#: `erp_names`: renames the second of two `Qty` headers by its position, and records what it was given.
ERP_NAMES = """
import json
from pathlib import Path

import sqlload as sl


@sl.rename
def erp_names(columns, ctx):
    seen = {"columns": [[column.position, column.source, column.default] for column in columns], "source": ctx.source, "table": ctx.table}
    (Path(__file__).resolve().parent.parent / "seen.json").write_text(json.dumps(seen))
    return {column.position: "qty_returned" for column in columns if column.source == "Qty" and column.position > 2}
"""


def make_project(tmp_path: Path, rename: str = "erp_names", module: str = ERP_NAMES) -> Path:
	(tmp_path / "renames").mkdir()
	(tmp_path / "drop").mkdir()
	(tmp_path / "sqlload.toml").write_text(PROJECT.format(rename=rename))
	(tmp_path / "renames" / "erp.py").write_text(textwrap.dedent(module))
	(tmp_path / "drop" / "orders.csv").write_text("Order ID,Qty,Qty\n1,5,2\n")
	return tmp_path


def sqlload(project: Path, *args: str) -> subprocess.CompletedProcess[str]:
	home = project / "home"
	home.mkdir(exist_ok=True)
	env = {**os.environ, "HOME": str(home), "XDG_CONFIG_HOME": str(home / ".config"), "XDG_CACHE_HOME": str(home / ".cache")}
	return subprocess.run(
		[sys.executable, "-c", "from sqlload._cli import main; main()", *args],
		cwd=project,
		env=env,
		capture_output=True,
		text=True,
	)


def columns(project: Path, table: str) -> list[tuple[Any, ...]]:
	with duckdb.connect(str(project / "wh.duckdb"), read_only=True) as connection:
		return connection.execute(
			f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'raw' AND table_name = '{table}' AND column_name NOT LIKE '\\_%' ESCAPE '\\' ORDER BY ordinal_position"
		).fetchall()


def test_a_rename_function_tells_repeated_headers_apart_by_position(tmp_path: Path) -> None:
	project = make_project(tmp_path)
	planned = sqlload(project, "plan")
	assert planned.returncode == 0, planned.stdout + planned.stderr
	assert "Qty (column 3) -> qty_returned (rename)" in planned.stdout, planned.stdout
	loaded = sqlload(project, "load")
	assert loaded.returncode == 0, loaded.stdout + loaded.stderr
	assert columns(project, "orders") == [("order_id",), ("qty",), ("qty_returned",)]
	assert (project / "seen.json").read_text() == (
		'{"columns": [[1, "Order ID", "order_id"], [2, "Qty", "qty"], [3, "Qty", "qty"]], "source": "orders", "table": "orders"}'
	)


OTHER = """
import sqlload as sl


@sl.rename("erp_names")
def other_names(columns, ctx):
    return {}
"""


@pytest.mark.parametrize(
	("rename", "other", "problem"),
	[
		(
			"missing_names",
			"",
			"rename missing_names: no function is registered as missing_names with @sl.rename; the registered ones are erp_names",
		),
		(
			"erp_names",
			OTHER,
			'rename erp_names: 2 functions are registered as erp_names, in renames/erp.py (erp_names) and renames/other.py (other_names); give one another name with @sl.rename("...")',
		),
	],
	ids=["unknown-name", "two-under-one-name"],
)
def test_plan_refuses_a_rename_function_it_cannot_look_up(tmp_path: Path, rename: str, other: str, problem: str) -> None:
	project = make_project(tmp_path, rename)
	if other:
		(project / "renames" / "other.py").write_text(other)
	planned = sqlload(project, "plan")
	assert planned.returncode == 1, planned.stdout + planned.stderr
	assert problem in planned.stdout + planned.stderr, planned.stdout + planned.stderr


def test_a_rename_that_makes_a_clash_fails_plan(tmp_path: Path) -> None:
	module = """
import sqlload as sl


@sl.rename
def erp_names(columns, ctx):
    return {1: "qty"}
"""
	project = make_project(tmp_path, module=module)
	planned = sqlload(project, "plan")
	assert planned.returncode == 1, planned.stdout + planned.stderr
	assert 'rename gives column "Order ID" the name qty, which column "Qty" (column 2) loads as too; give each a name of its own' in planned.stdout, planned.stdout


PYTHON_PROJECT = """\
python_paths = ["sources"]

[connections.wh]
kind = "duckdb"
database = "wh.duckdb"

[targets.dev]
connection = "wh"
loader_schema = "raw"
"""

#: A Python table passing its rename function itself, and one keeping its fields' names with `naming = "source"`.
SHOP = """
import sqlload as sl


@sl.rename("shop_names")
def shop_names(columns, ctx):
    return {column.position: "order_ref" for column in columns if column.source == "orderId"}


shop = sl.source("shop", write_strategy="replace")


@shop.table(description="Orders.", rename=shop_names)
def ShopOrders(ctx):
    yield {"orderId": 1, "rowNumber": 7}


@shop.table(description="Lines.", naming="source")
def lines(ctx):
    yield {"orderId": 1, "rowNumber": 7}
"""


def test_a_python_table_takes_the_rename_function_itself_and_snake_cases_the_rest(tmp_path: Path) -> None:
	(tmp_path / "sources").mkdir()
	(tmp_path / "sqlload.toml").write_text(PYTHON_PROJECT)
	(tmp_path / "sources" / "shop.py").write_text(SHOP)
	loaded = sqlload(tmp_path, "load")
	assert loaded.returncode == 0, loaded.stdout + loaded.stderr
	assert columns(tmp_path, "shop__shop_orders") == [("order_ref",), ("row_number",)]
	assert columns(tmp_path, "shop__lines") == [("orderId",), ("rowNumber",)]
