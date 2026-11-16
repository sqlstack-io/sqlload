"""Rows a Python table's function returns outside the cursor window it was asked for, and `timezone` for times without an offset, into DuckDB."""

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import duckdb
import pytest

PROJECT = """\
python_paths = ["sources"]

[connections.wh]
kind = "duckdb"
database = "wh.duckdb"

[targets.dev]
connection = "wh"
loader_schema = "raw"
"""

#: Hourly rows from 2026-02-20 to the end of 2026-03-15 (UTC), and functions that read them as APIs with different faults would.
MODULE = """
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import sqlload as sl

HERE = Path(__file__).resolve().parent.parent
shop = sl.source("shop", write_strategy="append", cursor_column="updated_at", cursor_start="2026-03-01 00:00:00", batch_size="7d"{source})

START = datetime(2026, 2, 20, tzinfo=timezone.utc)
ROWS = [{{"id": n, "updated_at": START + timedelta(hours=n)}} for n in range(24 * 24)]


def asked(ctx):
    calls = HERE / "calls.json"
    seen = json.loads(calls.read_text()) if calls.exists() else []
    calls.write_text(json.dumps(seen + [[repr(ctx.cursor_value), repr(ctx.cursor_end)]]))


@shop.table(description="An API that ignores the filter.")
def ignoring(ctx):
    asked(ctx)
    yield ROWS


@shop.table(description="A function that sends no end.")
def no_end(ctx):
    asked(ctx)
    yield [row for row in ROWS if row["updated_at"] >= ctx.cursor_value]


@shop.table(description="An API that filters by whole days.")
def whole_days(ctx):
    yield [row for row in ROWS if ctx.cursor_value <= row["updated_at"] and row["updated_at"].date() <= ctx.cursor_end.date()]


@shop.table(description="An API that reads the times it is sent as Paris time, whatever their offset.")
def paris(ctx):
    asked(ctx)
    start, end = (value.replace(tzinfo=ZoneInfo("Europe/Paris")) for value in (ctx.cursor_value, ctx.cursor_end))
    yield [row for row in ROWS if start <= row["updated_at"] < end]


@shop.table(description="An API that reads from its start inclusively.", {since})
def since(ctx):
    yield [row for row in ROWS if ctx.cursor_value <= row["updated_at"] < ctx.cursor_end]
"""


def make_project(tmp_path: Path, source: str = "", since: str = "write_strategy='append'") -> Path:
	(tmp_path / "sources").mkdir()
	(tmp_path / "sqlload.toml").write_text(PROJECT)
	(tmp_path / "sources" / "shop.py").write_text(MODULE.format(source=source and f", {source}", since=since))
	return tmp_path


def sqlload(project: Path, *args: str) -> subprocess.CompletedProcess[str]:
	home = project / "home"
	home.mkdir(exist_ok=True)
	env = {**os.environ, "HOME": str(home), "XDG_CONFIG_HOME": str(home / ".config"), "XDG_CACHE_HOME": str(home / ".cache")}
	return subprocess.run(
		[sys.executable, "-c", "from sqlload._cli import main; main()", "--no-color", *args], cwd=project, env=env, capture_output=True, text=True
	)


def load(project: Path, table: str, end: str = "2026-03-22 00:00:00") -> subprocess.CompletedProcess[str]:
	return sqlload(project, "load", "--end-cursor", end, f"shop__{table}")


def query(project: Path, sql: str) -> list[tuple[Any, ...]]:
	with duckdb.connect(str(project / "wh.duckdb"), read_only=True) as connection:
		return connection.execute(sql).fetchall()


def calls(project: Path) -> list[list[str]]:
	return json.loads((project / "calls.json").read_text())


@pytest.mark.parametrize(
	("table", "expected_message"),
	[
		pytest.param(
			"ignoring",
			"failed  raw.shop__ignoring  the function returned 408 rows outside the window it was asked for, "
			"updated_at >= 2026-03-01 00:00:00+00:00 and < 2026-03-08 00:00:00+00:00: 216 before its start and 192 after its end, "
			"2026-02-20 00:00:00+00:00 → 2026-03-15 23:00:00+00:00, the first at row 1 (updated_at 2026-02-20 00:00:00+00:00); "
			"the API may be ignoring the filter. Loading them would move the cursor past rows not read yet. "
			"Check the request's filter: an API ignores a parameter it does not know; "
			'on_cursor_out_of_range = "skip" drops rows outside the window, and "warn" loads them.',
			id="filter ignored",
		),
		pytest.param(
			"no_end",
			"failed  raw.shop__no_end  the function returned 192 rows outside the window it was asked for, "
			"updated_at >= 2026-03-01 00:00:00+00:00 and < 2026-03-08 00:00:00+00:00: 192 after its end, "
			"2026-03-08 00:00:00+00:00 → 2026-03-15 23:00:00+00:00, the first at row 169 (updated_at 2026-03-08 00:00:00+00:00); "
			"none before its start: the function may not send ctx.cursor_end, or the API may ignore it. "
			"Loading them would move the cursor past rows not read yet. Send ctx.cursor_end with the request; "
			'on_cursor_out_of_range = "skip" drops rows outside the window, and "warn" loads them.',
			id="no end sent",
		),
	],
)
def test_rows_outside_the_first_window_fail_the_table_with_their_count_and_likely_cause(tmp_path: Path, table: str, expected_message: str) -> None:
	project = make_project(tmp_path)
	done = load(project, table)
	assert done.returncode == 1, done.stdout + done.stderr
	assert expected_message in done.stdout, done.stdout
	assert len(calls(project)) == 1
	assert query(project, f'SELECT count(*) FROM raw."shop__{table}"') == [(0,)]


@pytest.mark.parametrize(
	("rule", "expected_rows", "expected_distinct", "expected_ending"),
	[
		("skip", 360, 360, 'skipped (on_cursor_out_of_range = "skip")'),
		("warn", 408, 360, 'loaded (on_cursor_out_of_range = "warn")'),
	],
)
def test_a_date_only_api_loads_each_boundary_day_once_under_skip_and_warns_of_it_under_warn(
	tmp_path: Path, rule: str, expected_rows: int, expected_distinct: int, expected_ending: str
) -> None:
	project = make_project(tmp_path, f'on_cursor_out_of_range="{rule}"')
	done = sqlload(project, "load", "--json", "--end-cursor", "2026-03-22 00:00:00", "shop__whole_days")
	assert done.returncode == 0, done.stdout + done.stderr
	(table,) = json.loads(done.stdout)["tables"]
	assert table["warnings"] == [
		"24 rows outside the window updated_at >= 2026-03-01 00:00:00+00:00 and < 2026-03-08 00:00:00+00:00: 24 after its end, "
		"2026-03-08 00:00:00+00:00 → 2026-03-08 23:00:00+00:00; all on 2026-03-08, the day the window ends: the API may filter by whole days; "
		+ expected_ending,
		"24 rows outside the window updated_at >= 2026-03-08 00:00:00+00:00 and < 2026-03-15 00:00:00+00:00: 24 after its end, "
		"2026-03-15 00:00:00+00:00 → 2026-03-15 23:00:00+00:00; all on 2026-03-15, the day the window ends: the API may filter by whole days; "
		+ expected_ending,
	]
	assert query(project, 'SELECT count(*), count(DISTINCT id), min(id), max(id) FROM raw."shop__whole_days"') == [(expected_rows, expected_distinct, 216, 575)]


def test_an_api_reading_times_in_another_zone_fails_as_an_offset_until_timezone_names_it(tmp_path: Path) -> None:
	project = make_project(tmp_path)
	done = load(project, "paris")
	assert done.returncode == 1, done.stdout + done.stderr
	assert (
		"1 before its start, at 2026-02-28 23:00:00+00:00, the first at row 1 (updated_at 2026-02-28 23:00:00+00:00); "
		"all within 1 hour before the window's start: an offset of 1 hour, as when the window is sent in another time zone than the API reads it in. "
		"Loading them would load again rows an earlier window read. "
		'Set timezone to the zone the API reads times in, such as timezone = "Europe/Paris"'
	) in done.stdout, done.stdout


def test_timezone_reads_cursor_start_and_flags_in_its_zone_and_gives_ctx_times_in_it(tmp_path: Path) -> None:
	project = make_project(tmp_path, 'timezone="Europe/Paris"')
	planned = sqlload(project, "plan", "-v", "shop__paris")
	assert (
		"reads   updated_at >= 2026-03-01 00:00:00 (first run, from cursor_start); rows outside a window fail the table; "
		"times without an offset in Europe/Paris (the source's timezone)"
	) in planned.stdout, planned.stdout
	done = load(project, "paris")
	assert done.returncode == 0, done.stdout + done.stderr
	assert calls(project)[0] == [
		"datetime.datetime(2026, 3, 1, 0, 0, tzinfo=zoneinfo.ZoneInfo(key='Europe/Paris'))",
		"datetime.datetime(2026, 3, 8, 0, 0, tzinfo=zoneinfo.ZoneInfo(key='Europe/Paris'))",
	]
	# Rows 215 to 575: 2026-02-28 23:00 UTC, midnight in Paris, to the last row.
	assert query(project, 'SELECT count(*), min(id), max(id) FROM raw."shop__paris"') == [(361, 215, 575)]


@pytest.mark.parametrize(
	("since", "expected_code", "expected_text"),
	[
		pytest.param(
			"write_strategy='append'",
			1,
			"1 before its start, at 2026-03-07 23:00:00+00:00, the first at row 1 (updated_at 2026-03-07 23:00:00+00:00); "
			"all at the window's start, which it leaves out: the API may include its start. "
			"Loading them would load again rows an earlier window read. "
			"Ask for rows after ctx.cursor_value when ctx.cursor_inclusive is False",
			id="append reads after its maximum",
		),
		pytest.param("write_strategy='append', append_cursor_inclusive=True", 0, "loaded  raw.shop__since", id="append_cursor_inclusive"),
		pytest.param("write_strategy='merge', unique_key='id', lookback='1h'", 0, "loaded  raw.shop__since", id="lookback"),
	],
)
def test_a_window_start_read_inclusively_is_not_outside_it(tmp_path: Path, since: str, expected_code: int, expected_text: str) -> None:
	project = make_project(tmp_path, since=since)
	assert load(project, "since", "2026-03-08 00:00:00").returncode == 0
	done = load(project, "since", "2026-03-15 00:00:00")
	assert done.returncode == expected_code, done.stdout + done.stderr
	assert expected_text in done.stdout, done.stdout
