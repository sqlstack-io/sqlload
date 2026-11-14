"""Transforms end to end: `transform` on files tables (each file or ZIP member) and on `google_sheets` tables (each range), into DuckDB."""

import json
import os
import subprocess
import sys
import textwrap
import threading
import time
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

PROJECT = """\
python_paths = ["transforms"]

[connections.wh]
kind = "duckdb"
database = "wh.duckdb"

[connections.files]
kind = "filesystem"
root = "."

[targets.dev]
connection = "wh"
loader_schema = "RAW"

[sources.metrics]
kind = "files"
connection = "files"
path = "drop/"
format = "{format}"
description = "Metrics, one file a month."
{settings}
[sources.metrics.tables.metrics]
transform = "transforms.metrics:transform"
{table}
"""

#: A transform returning each row of a CSV with `id` and `v` as dicts, a pyarrow Table or a polars DataFrame, with the file it came from.
RETURNS = {
	"dicts": """
def transform(table, ctx):
    return [{"id": int(row["id"]), "v": row["v"], "file": ctx.path} for row in table.to_pylist()]
""",
	"arrow": """
import pyarrow as pa
import pyarrow.compute as pc

def transform(table, ctx):
    return pa.table({"id": pc.cast(table["id"], pa.int64()), "v": table["v"], "file": [ctx.path] * table.num_rows})
""",
	"polars": """
import polars as pl

def transform(table, ctx):
    return pl.from_arrow(table).select(pl.col("id").cast(pl.Int64), pl.col("v"), pl.lit(ctx.path).alias("file"))
""",
}


def make_project(tmp_path: Path, module: str, *, format: str = "csv", settings: str = 'write_strategy = "append"\n', table: str = "") -> Path:
	(tmp_path / "transforms").mkdir()
	(tmp_path / "drop").mkdir()
	(tmp_path / "sqlload.toml").write_text(PROJECT.format(format=format, settings=settings, table=table))
	(tmp_path / "transforms" / "metrics.py").write_text(textwrap.dedent(module))
	return tmp_path


def give(project: Path, name: str, text: str, modified: float | None = None) -> None:
	path = project / "drop" / name
	path.write_text(text)
	if modified is not None:
		os.utime(path, (modified, modified))


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


def load(project: Path, *args: str) -> subprocess.CompletedProcess[str]:
	done = sqlload(project, "load", *args)
	assert done.returncode == 0, done.stdout + done.stderr
	return done


def query(project: Path, sql: str) -> list[tuple[Any, ...]]:
	with duckdb.connect(str(project / "wh.duckdb"), read_only=True) as connection:
		return connection.execute(sql).fetchall()


@pytest.mark.parametrize("returned", ["dicts", "arrow", "polars"])
def test_a_transform_returning_dicts_arrow_or_a_dataframe_loads_the_same_rows(tmp_path: Path, returned: str) -> None:
	project = make_project(tmp_path, RETURNS[returned], table='columns = { v = "decimal(6,2)" }')
	give(project, "a.csv", "id,v\n1,1.50\n2,2.25\n")
	give(project, "b.csv", "id,v\n3,3.75\n")
	load(project)
	rows = query(project, "SELECT id, CAST(v AS VARCHAR), file, _sqlload_source_file FROM RAW.metrics ORDER BY id")
	assert rows == [
		(1, "1.50", "./drop/a.csv", "./drop/a.csv"),
		(2, "2.25", "./drop/a.csv", "./drop/a.csv"),
		(3, "3.75", "./drop/b.csv", "./drop/b.csv"),
	]
	types = dict(query(project, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'metrics'"))
	assert (types["id"], types["v"]) == ("BIGINT", "DECIMAL(6,2)")


def test_each_zip_member_gets_its_own_call(tmp_path: Path) -> None:
	project = make_project(tmp_path, RETURNS["dicts"], settings='compression = "zip"\nwrite_strategy = "append"\n')
	with zipfile.ZipFile(project / "drop" / "batch.zip", "w") as archive:
		archive.writestr("a.csv", "id,v\n1,x\n2,y\n")
		archive.writestr("b.csv", "id,v\n3,z\n")
	load(project)
	rows = query(project, "SELECT file, _sqlload_source_file, count(*) FROM RAW.metrics GROUP BY ALL ORDER BY 1")
	assert rows == [("./drop/batch.zip/a.csv", "./drop/batch.zip/a.csv", 2), ("./drop/batch.zip/b.csv", "./drop/batch.zip/b.csv", 1)]


@pytest.mark.parametrize(
	("body", "error"),
	[
		('    if ctx.path.endswith("b.csv"):\n        raise ValueError("bad sheet")\n    return table\n', "ValueError: bad sheet"),
		('    if ctx.path.endswith("b.csv"):\n        return None\n    return table\n', "it returned NoneType"),
	],
	ids=["raises", "returns-none"],
)
def test_a_failing_transform_fails_its_file_which_neither_commits_nor_moves(tmp_path: Path, body: str, error: str) -> None:
	settings = 'write_strategy = "append"\nafter_load = { move_to = "done" }\n'
	project = make_project(tmp_path, "def transform(table, ctx):\n" + body, settings=settings)
	give(project, "a.csv", "id,v\n1,x\n")
	load(project)
	give(project, "b.csv", "id,v\n2,y\n")
	failed = sqlload(project, "load")
	assert failed.returncode == 1, failed.stdout + failed.stderr
	output = failed.stdout + failed.stderr
	assert "./drop/b.csv: transform transforms.metrics:transform failed" in output, output
	assert error in output, output
	assert sorted(path.name for path in (project / "drop").iterdir()) == ["b.csv"]
	assert query(project, "SELECT id FROM RAW.metrics ORDER BY id") == [("1",)]


def test_a_merge_still_merges_files_oldest_first_with_a_record_each(tmp_path: Path) -> None:
	settings = 'write_strategy = "merge"\nunique_key = ["id"]\n'
	project = make_project(tmp_path, RETURNS["dicts"], settings=settings)
	now = time.time()
	give(project, "a.csv", "id,v\n1,newest\n", modified=now)
	give(project, "b.csv", "id,v\n1,oldest\n2,b\n", modified=now - 200)
	give(project, "c.csv", "id,v\n1,middle\n3,c\n", modified=now - 100)
	load(project)
	assert query(project, "SELECT id, v FROM RAW.metrics ORDER BY id") == [(1, "newest"), (2, "b"), (3, "c")]
	merged = query(project, "SELECT count(*) FROM RAW._sqlload_runs WHERE note = 'files source merge'")
	assert merged == [(3,)]


@pytest.mark.parametrize(
	("transform", "problem"),
	[
		("transforms.missing:transform", "transform transforms.missing:transform: no module transforms.missing under python_paths (transforms)"),
		("transforms.metrics:nothing", "transform transforms.metrics:nothing: module transforms.metrics has no function nothing"),
		("transforms.metrics:LIMIT", "transform transforms.metrics:LIMIT: transforms.metrics.LIMIT is not callable"),
	],
	ids=["no-module", "no-function", "not-callable"],
)
def test_plan_refuses_a_transform_it_cannot_run_and_plans_other_tables(tmp_path: Path, transform: str, problem: str) -> None:
	project = make_project(tmp_path, "LIMIT = 3\n" + RETURNS["dicts"])
	config = project / "sqlload.toml"
	config.write_text(
		config.read_text().replace("transforms.metrics:transform", transform)
		+ '\n[sources.plain]\nkind = "files"\nconnection = "files"\npath = "plain/"\nformat = "csv"\ndescription = "Plain."\nwrite_strategy = "append"\n'
	)
	give(project, "a.csv", "id,v\n1,x\n")
	(project / "plain").mkdir()
	(project / "plain" / "p.csv").write_text("id\n1\n")
	planned = sqlload(project, "plan")
	assert planned.returncode == 1, planned.stdout + planned.stderr
	assert problem in planned.stdout, planned.stdout
	assert "└── plain" in planned.stdout or "── plain " in planned.stdout, planned.stdout


def test_plan_shows_the_transformed_columns_and_reads_the_files_itself(tmp_path: Path) -> None:
	module = """
def transform(table, ctx):
    return [{"metric": row["name"], "month": month, "value": value} for row in table.to_pylist() for month, value in row.items() if month != "name"]
"""
	project = make_project(tmp_path, module, format="parquet")
	pq.write_table(pa.table({"name": ["sales"], "jan": [1.5], "feb": [2.5]}), project / "drop" / "a.parquet")
	planned = sqlload(project, "plan")
	assert planned.returncode == 0, planned.stdout + planned.stderr
	lines = [line.strip("│├└─ ") for line in planned.stdout.splitlines()]
	assert [line.split()[0] for line in lines if line.split()[:1] in (["metric"], ["month"], ["value"])] == ["metric", "month", "value"], planned.stdout
	assert any(line.startswith("value") and "float" in line for line in lines), planned.stdout
	assert any(line.startswith("source") and line.endswith("transform transforms.metrics:transform") for line in lines), planned.stdout
	plain = project / "plain.toml"
	plain.write_text((project / "sqlload.toml").read_text().replace('transform = "transforms.metrics:transform"', ""))
	direct = sqlload(project, "plan", "-c", str(plain))
	route = [line for line in lines if line.startswith("route")]
	assert route != [line.strip("│├└─ ") for line in direct.stdout.splitlines() if line.strip("│├└─ ").startswith("route")], direct.stdout
	assert route == ["route   read parquet into Arrow -> DuckDB takes Arrow"], planned.stdout


def test_a_column_the_plan_did_not_see_fails_the_file_and_a_missing_one_loads_null(tmp_path: Path) -> None:
	module = """
def transform(table, ctx):
    rows = table.to_pylist()
    if ctx.path.endswith("a.csv"):
        return [{"id": int(row["id"])} for row in rows]
    if ctx.path.endswith("b.csv"):
        return [{"id": int(row["id"]), "v": row["v"], "extra": 1} for row in rows]
    return [{"id": int(row["id"]), "v": row["v"]} for row in rows]
"""
	project = make_project(tmp_path, module)
	give(project, "z.csv", "id,v\n1,x\n")
	load(project)
	give(project, "a.csv", "id,v\n2,y\n")
	load(project)
	assert query(project, "SELECT id, v FROM RAW.metrics ORDER BY id") == [(1, "x"), (2, None)]
	give(project, "b.csv", "id,v\n3,z\n")
	failed = sqlload(project, "load")
	assert failed.returncode == 1, failed.stdout + failed.stderr
	assert "returned column extra, which it did not return for ./drop/" in failed.stdout + failed.stderr, failed.stdout + failed.stderr


def test_the_load_report_gives_each_files_transform_time(tmp_path: Path) -> None:
	project = make_project(tmp_path, RETURNS["arrow"])
	give(project, "a.csv", "id,v\n1,x\n")
	give(project, "b.csv", "id,v\n2,y\n")
	done = load(project)
	report = [line.split()[:2] for line in done.stdout.splitlines() if line.strip().startswith("transform")]
	assert report == [["transform", "./drop/a.csv"], ["transform", "./drop/b.csv"]], done.stdout
	give(project, "c.csv", "id,v\n3,z\n")
	reported = json.loads(load(project, "--json").stdout)
	transforms = reported["tables"][0]["transforms"]
	assert [(item["file"], item["rows"], item["dicts"]) for item in transforms] == [("./drop/c.csv", 1, False)]


def test_plan_says_when_the_transform_changed_since_the_last_load(tmp_path: Path) -> None:
	project = make_project(tmp_path, RETURNS["dicts"])
	give(project, "a.csv", "id,v\n1,x\n")
	changed = "transform  changed since the last load"
	assert changed not in sqlload(project, "plan").stdout
	load(project)
	assert changed not in sqlload(project, "plan").stdout
	module = project / "transforms" / "metrics.py"
	module.write_text(module.read_text() + "\n# rows now keep their file\n")
	planned = sqlload(project, "plan")
	assert changed in planned.stdout, planned.stdout
	reloading = sqlload(project, "plan", "--reload")
	assert changed not in reloading.stdout, reloading.stdout



def test_a_changed_transform_reloads_a_merge_table_with_moved_files_into_one_row_per_key(tmp_path: Path) -> None:
	settings = 'write_strategy = "merge"\nunique_key = ["id"]\nafter_load = { move_to = "done" }\n'
	project = make_project(tmp_path, RETURNS["dicts"], settings=settings)
	now = time.time()
	give(project, "a.csv", "id,v\n1,a\n2,a\n", modified=now - 200)
	give(project, "b.csv", "id,v\n1,b\n", modified=now - 100)
	load(project)
	module = project / "transforms" / "metrics.py"
	module.write_text(module.read_text().replace('"v": row["v"]', '"v": row["v"].upper()'))
	give(project, "c.csv", "id,v\n3,c\n", modified=now)
	assert "transform  changed since the last load" in sqlload(project, "plan").stdout
	planned = sqlload(project, "plan", "--reload")
	assert "files   3 to read of 3 listed: 1 in ./drop/, 2 in ./done/" in planned.stdout, planned.stdout
	load(project, "--reload", "--yes")
	rows = query(project, "SELECT id, v, file FROM RAW.metrics ORDER BY id")
	assert rows == [(1, "B", "./drop/b.csv"), (2, "A", "./drop/a.csv"), (3, "C", "./drop/c.csv")]
	assert sorted(path.name for path in (project / "done").iterdir()) == ["a.csv", "b.csv", "c.csv"]
	give(project, "d.csv", "id,v\n4,d\n")
	after = sqlload(project, "plan").stdout
	assert "1 new of 1 listed" in after and "transform  changed since the last load" not in after, after

#: The `KPIs` tab, as the Sheets API writes it: a merged title row, a header row, label-only rows, KPI blocks with measures below each, a note and an `IFERROR` "" here and there.
def text(value: str) -> dict[str, Any]:
	return {"effectiveValue": {"stringValue": value}, "formattedValue": value}


def number(value: float, shown: str | None = None, kind: str = "") -> dict[str, Any]:
	cell: dict[str, Any] = {"effectiveValue": {"numberValue": value}, "formattedValue": shown if shown is not None else f"{value:g}"}
	if kind:
		cell["effectiveFormat"] = {"numberFormat": {"type": kind}}
	return cell


def percent(value: float) -> dict[str, Any]:
	return number(value, f"{value * 100:.1f}%", "PERCENT")


BLANK: dict[str, Any] = {}
EMPTY_TEXT = text("")

KPI_ROWS: list[list[dict[str, Any]]] = [
	[text("Service KPIs 2026"), BLANK, BLANK, BLANK, BLANK, BLANK, BLANK],
	[text("ID"), text("KPI"), text("Period 1"), text("Period 2"), text("Jan"), text("Feb"), text("Mar")],
	[BLANK, text("Customer service")],
	[BLANK, text("Targets reviewed quarterly"), text("note: P1 excludes returns")],
	[text("K1"), text("On-time delivery"), percent(0.9), EMPTY_TEXT, percent(0.95), number(0.8), EMPTY_TEXT],
	[BLANK, text("Delivered on time"), number(9), BLANK, number(19), number(8)],
	[BLANK, text("Delivered"), number(10), number(0), number(20), number(10), number(0)],
	[text("K2"), text("First contact resolution"), percent(0.5), percent(0.6), EMPTY_TEXT, percent(0.75), percent(1)],
	[BLANK, text("Resolved first time"), number(5), number(6), BLANK, number(3), number(2)],
	[BLANK, text("Contacts"), number(10), number(10), number(0), number(4), number(2)],
	[BLANK, text("Escalations"), number(1), text("n/a - system outage"), BLANK, number(0)],
	[],
	[text("K3"), text("Average handle time (min)"), number(6.5), number(7), number(6.25), BLANK, number(5.5)],
	[BLANK, text("Calls handled"), number(120), number(130), number(125), BLANK, number(118)],
]

TIDY_MODULE = """
import pandas as pd

MONTHS = ["Period 1", "Period 2", "Jan", "Feb", "Mar"]

def transform(table, ctx):
    df = table.to_pandas()
    kpi = df["ID"].notna()
    df["metric_id"] = df["ID"].ffill()
    df["metric"] = df["KPI"].where(kpi).ffill()
    df["label"] = df["KPI"]
    df["row_kind"] = kpi.map({True: "kpi", False: "measure"})
    df = df[df["metric_id"].notna()]
    tidy = df.melt(id_vars=["metric_id", "metric", "label", "row_kind"], value_vars=MONTHS, var_name="month", value_name="value")
    tidy["value"] = pd.to_numeric(tidy["value"], errors="coerce")
    return tidy.dropna(subset=["value"]).reset_index(drop=True)
"""

#: (metric_id, label, row_kind) and each month's value, worked out by hand from `KPI_ROWS`.
TIDY: dict[tuple[str, str, str], dict[str, float]] = {
	("K1", "On-time delivery", "kpi"): {"Period 1": 0.9, "Jan": 0.95, "Feb": 0.8},
	("K1", "Delivered on time", "measure"): {"Period 1": 9, "Jan": 19, "Feb": 8},
	("K1", "Delivered", "measure"): {"Period 1": 10, "Period 2": 0, "Jan": 20, "Feb": 10, "Mar": 0},
	("K2", "First contact resolution", "kpi"): {"Period 1": 0.5, "Period 2": 0.6, "Feb": 0.75, "Mar": 1},
	("K2", "Resolved first time", "measure"): {"Period 1": 5, "Period 2": 6, "Feb": 3, "Mar": 2},
	("K2", "Contacts", "measure"): {"Period 1": 10, "Period 2": 10, "Jan": 0, "Feb": 4, "Mar": 2},
	("K2", "Escalations", "measure"): {"Period 1": 1, "Feb": 0},
	("K3", "Average handle time (min)", "kpi"): {"Period 1": 6.5, "Period 2": 7, "Jan": 6.25, "Mar": 5.5},
	("K3", "Calls handled", "measure"): {"Period 1": 120, "Period 2": 130, "Jan": 125, "Mar": 118},
}
METRICS = {"K1": "On-time delivery", "K2": "First contact resolution", "K3": "Average handle time (min)"}


class FakeSheets(BaseHTTPRequestHandler):
	"""Hands out a token, and the `KPIs` tab for any spreadsheet and range."""

	def do_POST(self) -> None:
		self.rfile.read(int(self.headers.get("Content-Length", "0")))
		self.answer({"access_token": "t", "token_type": "Bearer", "expires_in": 3600})

	def do_GET(self) -> None:
		rows = [{"values": row} for row in KPI_ROWS]
		sheet = {"properties": {"title": "KPIs", "sheetType": "GRID"}, "data": [{"rowData": rows}]}
		self.answer({"properties": {"title": "Fake spreadsheet"}, "sheets": [sheet]})

	def answer(self, body: dict[str, Any]) -> None:
		data = json.dumps(body).encode()
		self.send_response(200)
		self.send_header("Content-Type", "application/json")
		self.send_header("Content-Length", str(len(data)))
		self.end_headers()
		self.wfile.write(data)

	def log_message(self, format: str, *args: Any) -> None:
		return


def test_a_sheets_range_goes_through_its_transform_into_tidy_rows(tmp_path: Path) -> None:
	server = ThreadingHTTPServer(("127.0.0.1", 0), FakeSheets)
	threading.Thread(target=server.serve_forever, daemon=True).start()
	root = f"http://127.0.0.1:{server.server_address[1]}"
	try:
		(tmp_path / "transforms").mkdir()
		(tmp_path / "transforms" / "kpis.py").write_text(TIDY_MODULE)
		(tmp_path / "sqlload.toml").write_text(
			f"""\
python_paths = ["transforms"]

[connections.sheets]
kind = "google_sheets"
client_id = "client.apps.googleusercontent.com"
client_secret = "secret"
refresh_token = "refresh"
base_url = "{root}/v4/spreadsheets"
token_url = "{root}/token"

[connections.wh]
kind = "duckdb"
database = "wh.duckdb"

[targets.dev]
connection = "wh"
loader_schema = "RAW"

[sources.kpis]
kind = "google_sheets"
connection = "sheets"
spreadsheet_id = "synthetic"
ranges = {{ metrics = "KPIs" }}
header_row = 2
description = "Service KPIs, tidied."
write_strategy = "replace"
transform = "transforms.kpis:transform"
"""
		)
		done = load(tmp_path)
		assert "transform  KPIs" in done.stdout, done.stdout
	finally:
		server.shutdown()
	rows = query(tmp_path, "SELECT metric_id, metric, label, row_kind, month, value FROM RAW.kpis__metrics")
	expected = [
		(metric_id, METRICS[metric_id], label, kind, month, float(value))
		for (metric_id, label, kind), months in TIDY.items()
		for month, value in months.items()
	]
	assert len(expected) == 34
	assert sorted(rows) == sorted(expected)
