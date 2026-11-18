"""Peak memory: the run's in every `load --json`, and each table's with `--memory-per-table` (Linux), on a Python source; and a budget too small for any table."""

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import duckdb
import pytest

PROJECT = """\
python_paths = ["sources"]

[connections.wh]
kind = "duckdb"
database = "wh.duckdb"

[targets.dev]
connection = "wh"
loader_schema = "RAW"
"""

#: Two tables loading in name order: `a_first` and `b_second`, one of them `small` (ten rows) and the other `large` (an Arrow table of about 160 MB, built in the table's own call).
MODULE = """\
import pyarrow as pa
import sqlload as sl


def small(ctx):
    yield pa.table({{"id": list(range(10))}})


def large(ctx):
    rows = 10_000_000
    yield pa.table({{"id": pa.array(range(rows), pa.int64()), "v": pa.array(range(rows), pa.int64())}})


@sl.table(description="The first table to load.", write_strategy="replace")
def a_first(ctx):
    yield from {first}(ctx)


@sl.table(description="The second table to load.", write_strategy="replace")
def b_second(ctx):
    yield from {second}(ctx)
"""

MEGABYTE = 1_000_000


def peaks(tmp_path: Path, first: str, second: str) -> tuple[dict[str, int], int]:
	"""`load --json --memory-per-table` on a project whose first table is `first` and second `second`: each table's peak and the run's."""
	(tmp_path / "sources").mkdir()
	(tmp_path / "sqlload.toml").write_text(PROJECT)
	(tmp_path / "sources" / "shop.py").write_text(textwrap.dedent(MODULE.format(first=first, second=second)))
	home = tmp_path / "home"
	home.mkdir()
	env = {**os.environ, "HOME": str(home), "XDG_CONFIG_HOME": str(home / ".config"), "XDG_CACHE_HOME": str(home / ".cache")}
	done = subprocess.run(
		[sys.executable, "-c", "from sqlload._cli import main; main()", "load", "--json", "--memory-per-table"],
		cwd=tmp_path,
		env=env,
		capture_output=True,
		text=True,
	)
	assert done.returncode == 0, done.stdout + done.stderr
	report = json.loads(done.stdout)
	tables = {table["table"]: table["peak_memory_bytes"] for table in report["tables"]}
	return tables, report["summary"]["peak_memory_bytes"]


@pytest.mark.skipif(sys.platform != "linux", reason="--memory-per-table needs Linux")
@pytest.mark.parametrize(("first", "second"), [("small", "large"), ("large", "small")])
def test_memory_per_table_measures_each_table_from_its_own_start(tmp_path: Path, first: str, second: str) -> None:
	tables, run = peaks(tmp_path, first, second)
	first_peak, second_peak = tables["RAW.a_first"], tables["RAW.b_second"]
	print(f"{first} then {second}: first {first_peak:,} bytes, second {second_peak:,} bytes, run {run:,} bytes")
	assert run >= max(first_peak, second_peak) >= MEGABYTE
	large, small = (second_peak, first_peak) if second == "large" else (first_peak, second_peak)
	# Building and loading the large table counts in its own peak; the reset before the small table keeps it out of the small one's, apart from what the process still holds.
	assert large - small > 300 * MEGABYTE, (first, second, tables)



@pytest.mark.parametrize("json_report", [False, True])
def test_a_budget_too_small_for_any_table_stops_the_load_before_it_writes(tmp_path: Path, json_report: bool) -> None:
	"""A 256MB budget leaves the tables nothing after DuckDB's share: `load` refuses like `plan`, creating and loading no table, and still reports as a load."""
	(tmp_path / "sources").mkdir()
	(tmp_path / "sqlload.toml").write_text(PROJECT)
	(tmp_path / "sources" / "shop.py").write_text(textwrap.dedent(MODULE.format(first="small", second="small")))
	home = tmp_path / "home"
	home.mkdir()
	env = {**os.environ, "HOME": str(home), "XDG_CONFIG_HOME": str(home / ".config"), "XDG_CACHE_HOME": str(home / ".cache")}
	done = subprocess.run(
		[sys.executable, "-c", "from sqlload._cli import main; main()", "load", "--memory-limit", "256MB", *(["--json"] if json_report else [])],
		cwd=tmp_path,
		env=env,
		capture_output=True,
		text=True,
	)
	output = done.stdout + done.stderr
	assert done.returncode == 1, output
	if json_report:
		report = json.loads(done.stdout)
		assert report["command"] == "load", report
		assert report["summary"]["loaded"] == 0, report["summary"]
		assert any("raise memory_limit to at least" in json.dumps(error) for error in report["plan"]["errors"]), report["plan"]["errors"]
	else:
		assert "raise memory_limit to at least" in output, output
	with duckdb.connect(str(tmp_path / "wh.duckdb")) as connection:
		tables = connection.execute("SELECT table_schema, table_name FROM information_schema.tables").fetchall()
	assert tables == [], tables
