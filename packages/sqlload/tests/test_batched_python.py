"""Python tables loading batch by batch: commits along the way, resuming after a failure, windows, bounded memory."""

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq
import pytest
import sqlload as sqlload_package

PROJECT = """\
python_paths = ["sources"]

[connections.wh]
kind = "duckdb"
path = "wh.duckdb"

[targets.dev]
connection = "wh"
loader_schema = "RAW"
"""

FILES_PROJECT = """\
python_paths = ["sources"]

[connections.lake]
kind = "filesystem"
root = "lake"

[targets.lake]
connection = "lake"
path = "{table}/"
"""

GROWING_MODULE = """
import pyarrow as pa
import sqlload as sl

api = sl.source("api")


@api.table(write_strategy="append")
def growing(ctx):
    yield pa.table({"id": [1]})
    yield pa.table({"id": [2], "b": [1]})
    yield pa.table({"id": [3], "b": [2], "c": [1]})
"""

#: Pages of 700 rows yielded one row at a time, so the 1,000-row crossings never line up with them.
#: `before` sets ctx.state to the next page before yielding a page, `after` once it is yielded,
#: `ended` only at the end; each raises at the page data/fail.json names. `windowed` records each
#: call's window; `big` yields many rows.
MODULE = """
import json
import time
from pathlib import Path

import pyarrow as pa
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


@api.table(write_strategy="append", state="logged")
def mutated_before(ctx):
    for page in range((ctx.state or {}).get("page", 0), PAGES):
        if page == fail_at():
            raise RuntimeError(f"page {page} failed")
        s = ctx.state or {}
        s["page"] = page + 1
        ctx.state = s
        yield from rows(page)


@api.table(write_strategy="append", state="logged")
def mutated_after(ctx):
    for page in range((ctx.state or {}).get("page", 0), PAGES):
        if page == fail_at():
            raise RuntimeError(f"page {page} failed")
        yield from rows(page)
        s = ctx.state or {}
        s["page"] = page + 1
        ctx.state = s


@api.table(write_strategy="append", state="logged")
def changed_in_place(ctx):
    ctx.state = ctx.state or {"token": 0}
    ctx.state["token"] += 1
    yield {"page": 0, "n": ctx.state["token"]}


@api.table(write_strategy="delete_insert", cursor_column="day", cursor_start="2026-11-01", batch="1d")
def windowed(ctx):
    calls = HERE / "data" / "calls.json"
    seen = json.loads(calls.read_text()) if calls.exists() else []
    seen.append([str(ctx.cursor_value), str(ctx.cursor_end)])
    calls.write_text(json.dumps(seen))
    yield {"day": str(ctx.cursor_value), "n": len(seen)}


@api.table(write_strategy="append")
def appended(ctx):
    for page in range(PAGES):
        if page == fail_at():
            raise RuntimeError(f"page {page} failed")
        yield rows(page)


@api.table(write_strategy="append")
def growing(ctx):
    yield pa.table({"id": [1]})
    yield pa.table({"id": [2], "b": [1]})
    yield pa.table({"id": [3], "b": [2], "c": [1]})


def wait_for(name):
    deadline = time.monotonic() + 20
    while not (HERE / "data" / name).exists():
        if time.monotonic() > deadline:
            raise RuntimeError(f"nothing wrote {name}")
        time.sleep(0.02)


@api.table(write_strategy="append")
def a_slow(ctx):
    wait_for("c_small")
    yield {"id": 1}


@api.table(write_strategy="append")
def b_small(ctx):
    yield {"id": 2}


@api.table(write_strategy="append")
def c_small(ctx):
    (HERE / "data" / "c_small").touch()
    yield {"id": 3}


fresh = sl.source("fresh", schema="NEW_SCHEMA", write_strategy="append")
for number in range(8):
    def _one(ctx, number=number):
        yield {"id": number}
    fresh.table(name=f"t{number}")(_one)


shared = sl.source("shared", write_strategy="append")


@shared.client
def shared_client(ctx):
    return object()


def alone(ctx):
    ctx.client
    lock = HERE / "data" / "shared.lock"
    lock.mkdir()
    time.sleep(0.2)
    lock.rmdir()
    yield {"id": 1}


shared.table(name="one")(alone)
shared.table(name="two")(alone)
shared.table(name="three")(alone)

together = sl.source("together", write_strategy="append", parallel_tables=True)


@together.client
def together_client(ctx):
    return object()


def beside(other):
    def function(ctx):
        ctx.client
        (HERE / "data" / ctx.table).touch()
        wait_for(other)
        yield {"id": 1}
    return function


together.table(name="left")(beside("right"))
together.table(name="right")(beside("left"))


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


def sqlload(project: Path, *args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
	home = project / "home"
	home.mkdir(exist_ok=True)
	variables = {
		**os.environ,
		**(env or {}),
		"HOME": str(home),
		"XDG_CONFIG_HOME": str(home / ".config"),
		"XDG_CACHE_HOME": str(home / ".cache"),
		"TMPDIR": str(project / "tmp"),
	}
	return subprocess.run(
		[sys.executable, "-c", "from sqlload._cli import main; main()", *args], cwd=project, env=variables, capture_output=True, text=True
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
	[("before", 2 * PAGE_ROWS), ("after", 2 * PAGE_ROWS), ("ended", 0), ("mutated_before", 2 * PAGE_ROWS), ("mutated_after", 2 * PAGE_ROWS)],
	ids=[
		"state set before each page",
		"state set after each page",
		"state set only at the end",
		"one object mutated and assigned again before each page",
		"one object mutated and assigned again after each page",
	],
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


def test_given_append_without_a_cursor_when_a_rerun_follows_a_failure_then_no_row_is_duplicated(project: Path) -> None:
	fail(project, 3)
	failed = sqlload(project, "load", "--batch-bytes", "1", "api__appended")
	assert failed.returncode != 0, failed.stdout
	assert "one commit for the whole read (append without a cursor" in failed.stdout + failed.stderr
	assert query(project, 'SELECT count(*) FROM "RAW"."api__appended"') == [(0,)]
	fail(project, -1)
	done = sqlload(project, "load", "--batch-bytes", "1", "api__appended")
	assert done.returncode == 0, done.stdout + done.stderr
	assert sorted(rows_loaded(project, "appended")) == sorted(EVERY_ROW)


def test_given_columns_added_over_batches_when_loading_then_each_change_reads_the_live_columns_once(project: Path) -> None:
	done = sqlload(project, "load", "api__growing", env={"RUST_LOG": "sqlload=debug"})
	assert done.returncode == 0, done.stdout + done.stderr
	reads = [line for line in done.stderr.splitlines() if "reading the table's live columns" in line and "api__growing" in line]
	assert len(reads) == 3, done.stderr
	assert query(project, 'SELECT id, b, c FROM "RAW"."api__growing" ORDER BY id') == [(1, None, None), (2, 1, None), (3, 2, 1)]


def test_given_a_slow_table_when_loading_at_once_then_it_does_not_hold_up_the_small_ones(project: Path) -> None:
	done = sqlload(project, "load", "--threads", "2", "api__a_slow", "api__b_small", "api__c_small")
	assert done.returncode == 0, done.stdout + done.stderr
	loaded = [line.split()[1] for line in done.stdout.splitlines() if line.startswith("loaded")]
	assert loaded == ["RAW.api__a_slow", "RAW.api__b_small", "RAW.api__c_small"]


def test_given_parallel_tables_creating_a_new_schema_when_loading_then_every_table_loads(project: Path) -> None:
	names = [f"fresh__t{number}" for number in range(8)]
	done = sqlload(project, "load", "--threads", "8", *names)
	assert done.returncode == 0, done.stdout + done.stderr
	for number, name in enumerate(names):
		assert query(project, f'SELECT id FROM "NEW_SCHEMA"."{name}"') == [(number,)]


def test_given_a_source_sharing_a_client_when_loading_then_its_tables_run_one_at_a_time_unless_it_opts_in(project: Path) -> None:
	alone = sqlload(project, "load", "--threads", "3", "shared__one", "shared__two", "shared__three")
	assert alone.returncode == 0, alone.stdout + alone.stderr
	beside = sqlload(project, "load", "--threads", "2", "together__left", "together__right")
	assert beside.returncode == 0, beside.stdout + beside.stderr


@pytest.mark.parametrize(
	("settings", "expected_error", "expected_message"),
	[
		("sl.source('s', parallel_tables='yes')", TypeError, "parallel_tables must be True or False"),
		("sl.table(parallel_tables=True)", TypeError, "parallel_tables"),
	],
	ids=["not a bool", "on a table"],
)
def test_given_a_bad_parallel_tables_when_declaring_then_it_is_refused(settings: str, expected_error: type[Exception], expected_message: str) -> None:
	with pytest.raises(expected_error, match=expected_message):
		eval(settings, {"sl": sqlload_package})


def test_given_a_load_when_it_starts_then_staging_tables_older_than_a_day_are_dropped(project: Path) -> None:
	recent = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y%m%d%H%M%S")
	with duckdb.connect(str(project / "wh.duckdb")) as connection:
		connection.execute('CREATE SCHEMA "RAW"')
		for name in ["x__sqlload_staging_20200101000000_abcd1234", f"x__sqlload_staging_{recent}_abcd1234", "x_keep"]:
			connection.execute(f'CREATE TABLE "RAW"."{name}" (id INT)')
	done = sqlload(project, "load", "api__b_small")
	assert done.returncode == 0, done.stdout + done.stderr
	found = [name for (name,) in query(project, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'RAW' AND table_name LIKE 'x%' ORDER BY 1")]
	assert found == sorted([f"x__sqlload_staging_{recent}_abcd1234", "x_keep"])


def test_given_a_selector_matching_no_table_when_planning_then_it_warns_with_the_nearest_name(project: Path) -> None:
	done = sqlload(project, "plan", "api__b_smal")
	assert done.returncode == 0, done.stdout + done.stderr
	assert "no table matches api__b_smal; did you mean api__b_small?" in done.stdout


def test_given_batch_whole_when_planning_a_python_table_then_the_plan_says_so(project: Path) -> None:
	(project / "sources" / "whole.py").write_text(
		"import sqlload as sl\n"
		"whole = sl.source('whole')\n"
		"@whole.table(write_strategy='delete_insert', cursor_column='day', cursor_start='2026-11-01', batch='whole')\n"
		"def days(ctx):\n"
		"    yield {'day': '2026-11-01'}\n"
	)
	done = sqlload(project, "plan", "whole__days")
	assert done.returncode == 0, done.stdout + done.stderr
	assert 'one commit for the whole read (batch = "whole")' in done.stdout


def test_given_a_column_added_mid_commit_when_loading_into_files_then_no_value_is_dropped(project: Path) -> None:
	(project / "sqlload.toml").write_text(FILES_PROJECT)
	(project / "sources" / "api.py").write_text(GROWING_MODULE)
	done = sqlload(project, "load", "api__growing")
	assert done.returncode == 0, done.stdout + done.stderr
	rows = [row for path in sorted((project / "lake").rglob("*.parquet")) for row in pq.read_table(path).to_pylist()]
	assert sorted((row["id"], row.get("b"), row.get("c")) for row in rows) == [(1, None, None), (2, 1, None), (3, 2, 1)]


def test_given_delete_insert_when_a_read_yields_no_rows_then_rows_deleted_upstream_go(project: Path) -> None:
	(project / "sources" / "days.py").write_text(
		"import json\n"
		"from pathlib import Path\n"
		"import sqlload as sl\n"
		"HERE = Path(__file__).resolve().parent.parent\n"
		"days = sl.source('days')\n"
		"@days.table(write_strategy='delete_insert', cursor_column='day', columns={'day': 'date'}, cursor_start='2026-11-01')\n"
		"def rows(ctx):\n"
		"    yield from json.loads((HERE / 'data' / 'rows.json').read_text())\n"
	)
	(project / "data" / "rows.json").write_text('[{"day": "2026-11-01", "id": 1}, {"day": "2026-11-02", "id": 2}]')
	assert sqlload(project, "load", "days__rows").returncode == 0
	(project / "data" / "rows.json").write_text("[]")
	done = sqlload(project, "load", "days__rows")
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT id FROM "RAW"."days__rows"') == [(1,)]


def test_given_a_state_changed_in_place_without_assigning_when_the_run_ends_then_it_is_recorded(project: Path) -> None:
	for _ in range(2):
		done = sqlload(project, "load", "api__changed_in_place")
		assert done.returncode == 0, done.stdout + done.stderr
	assert [state for (_, state) in records(project, "changed_in_place")] == ['{"token": 1}', '{"token": 2}']


def test_given_a_first_load_with_no_rows_when_its_schema_is_new_then_no_schema_is_created(project: Path) -> None:
	(project / "sources" / "empty.py").write_text(
		"import sqlload as sl\n"
		"empty = sl.source('empty', schema='NEVER_MADE')\n"
		"@empty.table(write_strategy='append')\n"
		"def nothing(ctx):\n"
		"    return\n"
		"    yield\n"
	)
	done = sqlload(project, "load", "empty__nothing")
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'NEVER_MADE'") == []


def test_given_shaping_when_declaring_a_python_table_then_it_is_refused_as_an_unknown_setting(project: Path) -> None:
	(project / "sources" / "shaped.py").write_text(
		"import sqlload as sl\n"
		"shaped = sl.source('shaped')\n"
		"@shaped.table(write_strategy='append', shaping='loader')\n"
		"def rows(ctx):\n"
		"    yield {'id': 1}\n"
	)
	refused = sqlload(project, "plan", "shaped__rows")
	assert refused.returncode != 0
	assert "shaping" in refused.stdout + refused.stderr
