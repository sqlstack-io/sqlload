"""Python tables loading batch by batch: commits along the way, resuming after a failure, windows, bounded memory."""

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
path = "wh.duckdb"

[targets.dev]
connection = "wh"
loader_schema = "RAW"
"""

#: Pages of 700 rows yielded one row at a time, so the 1,000-row crossings never line up with them.
#: `before` sets ctx.state to the next page before yielding a page, `after` once it is yielded,
#: `ended` only at the end; each raises at the page data/fail.json names. `windowed` records each
#: call's window; `big` yields many rows.
MODULE = """
import json
from pathlib import Path

import sqlload as sl

HERE = Path(__file__).resolve().parent.parent
api = sl.source("api")
PAGES = 6
ROWS = 700


def fail_at():
    return json.loads((HERE / "data" / "fail.json").read_text())


def rows(page):
    return [{"page": page, "n": n} for n in range(ROWS)]


@api.table(write_strategy="append", state="logged")
def before(ctx):
    for page in range(ctx.state or 0, PAGES):
        if page == fail_at():
            raise RuntimeError(f"page {page} failed")
        ctx.state = page + 1
        yield from rows(page)


@api.table(write_strategy="append", state="logged")
def after(ctx):
    for page in range(ctx.state or 0, PAGES):
        if page == fail_at():
            raise RuntimeError(f"page {page} failed")
        yield from rows(page)
        ctx.state = page + 1


@api.table(write_strategy="append", state="logged")
def ended(ctx):
    page = ctx.state or 0
    for page in range(page, PAGES):
        if page == fail_at():
            raise RuntimeError(f"page {page} failed")
        yield from rows(page)
    ctx.state = PAGES


@api.table(write_strategy="delete_insert", cursor_column="day", cursor_start="2026-11-01", batch="1d")
def windowed(ctx):
    calls = HERE / "data" / "calls.json"
    seen = json.loads(calls.read_text()) if calls.exists() else []
    seen.append([str(ctx.cursor_value), str(ctx.cursor_end)])
    calls.write_text(json.dumps(seen))
    yield {"day": str(ctx.cursor_value), "n": len(seen)}


@api.table(write_strategy="merge", unique_key=["page", "n"])
def big(ctx):
    for page in range(200):
        yield [{"page": page, "n": n, "pad": "x" * 200} for n in range(1000)]
"""


@pytest.fixture
def project(tmp_path: Path) -> Path:
	for folder in ["sources", "data", "tmp"]:
		(tmp_path / folder).mkdir()
	(tmp_path / "sqlload.toml").write_text(PROJECT)
	(tmp_path / "sources" / "api.py").write_text(MODULE)
	fail(tmp_path, -1)
	return tmp_path


def fail(project: Path, page: int) -> None:
	(project / "data" / "fail.json").write_text(json.dumps(page))


def sqlload(project: Path, *args: str) -> subprocess.CompletedProcess[str]:
	home = project / "home"
	home.mkdir(exist_ok=True)
	env = {
		**os.environ,
		"HOME": str(home),
		"XDG_CONFIG_HOME": str(home / ".config"),
		"XDG_CACHE_HOME": str(home / ".cache"),
		"TMPDIR": str(project / "tmp"),
	}
	return subprocess.run(
		[sys.executable, "-c", "from sqlload._cli import main; main()", *args], cwd=project, env=env, capture_output=True, text=True
	)


def query(project: Path, sql: str) -> list[tuple[Any, ...]]:
	with duckdb.connect(str(project / "wh.duckdb"), read_only=True) as connection:
		return connection.execute(sql).fetchall()


def records(project: Path, table: str) -> list[tuple[Any, Any]]:
	rows = query(project, f"SELECT record_json FROM \"RAW\"._sqlload_runs WHERE table_name = 'api__{table}' ORDER BY recorded_at")
	return [(json.loads(text).get("batch"), json.loads(text)["state"]) for (text,) in rows]


PAGE_ROWS = 700
EVERY_ROW = {(page, n) for page in range(6) for n in range(PAGE_ROWS)}


def rows_loaded(project: Path, table: str) -> list[tuple[int, int]]:
	return query(project, f'SELECT page, n FROM "RAW"."api__{table}"')


@pytest.mark.parametrize(
	("table", "expected_most_extra_rows"),
	[("before", 2 * PAGE_ROWS), ("after", 2 * PAGE_ROWS), ("ended", 0)],
	ids=["state set before each page", "state set after each page", "state set only at the end"],
)
def test_given_a_logged_table_when_a_page_fails_mid_run_then_the_next_run_loses_no_row_and_reloads_at_most_two_pages(
	project: Path, table: str, expected_most_extra_rows: int
) -> None:
	fail(project, 4)
	failed = sqlload(project, "load", "--batch-bytes", "1", f"api__{table}")
	assert failed.returncode != 0, failed.stdout
	assert "page 4 failed" in failed.stdout + failed.stderr
	fail(project, -1)
	done = sqlload(project, "load", "--batch-bytes", "1", f"api__{table}")
	assert done.returncode == 0, done.stdout + done.stderr
	loaded = rows_loaded(project, table)
	assert set(loaded) == EVERY_ROW
	assert len(loaded) - len(EVERY_ROW) <= expected_most_extra_rows


def test_given_a_logged_table_setting_state_per_page_when_loading_then_each_commit_has_a_numbered_record(project: Path) -> None:
	done = sqlload(project, "load", "--batch-bytes", "1", "api__after")
	assert done.returncode == 0, done.stdout + done.stderr
	numbered = records(project, "after")
	assert [batch for (batch, _) in numbered] == list(range(1, len(numbered) + 1))
	assert len(numbered) > 1
	assert numbered[-1][1] == "6"
	assert sorted(rows_loaded(project, "after")) == sorted(EVERY_ROW)


def test_given_batch_windows_when_loading_a_python_table_then_its_function_is_called_once_per_window(project: Path) -> None:
	done = sqlload(project, "load", "--end-cursor-ts", "2026-11-04", "api__windowed")
	assert done.returncode == 0, done.stdout + done.stderr
	calls = json.loads((project / "data" / "calls.json").read_text())
	assert calls == [["2026-11-01", "2026-11-02"], ["2026-11-02", "2026-11-03"], ["2026-11-03", "2026-11-04"]]
	assert query(project, 'SELECT day, n FROM "RAW"."api__windowed" ORDER BY day') == [
		("2026-11-01", 1),
		("2026-11-02", 2),
		("2026-11-03", 3),
	]


def test_given_a_large_generator_when_loading_then_batches_commit_along_the_way_without_spilling(project: Path) -> None:
	done = sqlload(project, "load", "--batch-bytes", "8000000", "api__big")
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT count(*), count(DISTINCT page) FROM "RAW"."api__big"') == [(200_000, 200)]
	assert [path.name for path in (project / "tmp").iterdir() if path.name.startswith("sqlload-python-")] == []
	assert "committing about every 1 GB" in sqlload(project, "plan", "api__big").stdout
