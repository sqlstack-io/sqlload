"""`state = "logged"` end to end: the run log in DuckDB and in a files target, and `sqlload state`."""

import json
import os
import subprocess
import sys
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

#: `pages` reads data/pages.json, a list of pages, from the page in ctx.state, and records what it saw;
#: `events` yields data/events.json (rows with a cursor `at`), recording ctx.cursor_value.
MODULE = """
import json
from pathlib import Path

import sqlload as sl

HERE = Path(__file__).resolve().parent.parent
api = sl.source("api")


def remember(ctx, **seen):
    (HERE / "seen").mkdir(exist_ok=True)
    (HERE / "seen" / f"{ctx.table}.json").write_text(json.dumps({"state": ctx.state, "is_reload": ctx.is_reload, **seen}))


@api.table(write_strategy="append", state="logged")
def pages(ctx):
    remember(ctx)
    data = json.loads((HERE / "data" / "pages.json").read_text())
    page = ctx.state or 0
    for rows in data[page:]:
        yield rows
        page += 1
    ctx.state = page


@api.table(write_strategy="append", cursor_column="at", state="logged")
def events(ctx):
    remember(ctx, cursor_value=repr(ctx.cursor_value))
    yield from json.loads((HERE / "data" / "events.json").read_text())
"""


def make_project(tmp_path: Path, toml: str) -> Path:
	for folder in ["sources", "data", "lake"]:
		(tmp_path / folder).mkdir()
	(tmp_path / "sqlload.toml").write_text(toml)
	(tmp_path / "sources" / "api.py").write_text(MODULE)
	give(tmp_path, "pages", [])
	give(tmp_path, "events", [])
	return tmp_path


def give(project: Path, table: str, rows: Any) -> None:
	(project / "data" / f"{table}.json").write_text(json.dumps(rows))


def sqlload(project: Path, *args: str) -> subprocess.CompletedProcess[str]:
	home = project / "home"
	home.mkdir(exist_ok=True)
	env = {**os.environ, "HOME": str(home), "XDG_CONFIG_HOME": str(home / ".config"), "XDG_CACHE_HOME": str(home / ".cache")}
	return subprocess.run(
		[sys.executable, "-c", "from sqlload._cli import main; main()", *args], cwd=project, env=env, capture_output=True, text=True
	)


def ok(project: Path, *args: str) -> str:
	done = sqlload(project, *args)
	assert done.returncode == 0, done.stdout + done.stderr
	return done.stdout


def seen(project: Path, table: str) -> dict[str, Any]:
	return json.loads((project / "seen" / f"{table}.json").read_text())


def query(project: Path, sql: str) -> list[tuple[Any, ...]]:
	with duckdb.connect(str(project / "wh.duckdb"), read_only=True) as connection:
		return connection.execute(sql).fetchall()


def execute(project: Path, sql: str) -> None:
	with duckdb.connect(str(project / "wh.duckdb")) as connection:
		connection.execute(sql)


def runs(project: Path, table: str) -> list[tuple[Any, ...]]:
	return query(
		project, f"SELECT event, state, cursor_after, row_count FROM \"RAW\".\"_sqlload_runs\" WHERE table_name = '{table}' ORDER BY recorded_at"
	)


def file_runs(project: Path, table: str) -> list[dict[str, Any]]:
	folder = project / "lake" / "_sqlload" / "runs" / table
	records = [json.loads(path.read_text()) for path in sorted(folder.glob("*.json"))] if folder.is_dir() else []
	return sorted(records, key=lambda record: record["recorded_at"])


def file_values(project: Path, table: str) -> list[Any]:
	rows = ds.dataset(project / "lake" / table, format="parquet").to_table().to_pylist()
	return sorted(row["n"] for row in rows)


def test_given_a_logged_table_when_loading_twice_then_the_second_run_resumes_from_the_logged_state(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT)
	give(project, "pages", [[{"n": 1}], [{"n": 2}]])
	ok(project, "load", "api__pages")
	assert seen(project, "pages")["state"] is None
	give(project, "pages", [[{"n": 1}], [{"n": 2}], [{"n": 3}]])
	ok(project, "load", "api__pages")
	assert seen(project, "pages")["state"] == 2
	assert query(project, 'SELECT n FROM "RAW"."api__pages" ORDER BY n') == [(1,), (2,), (3,)]
	assert runs(project, "api__pages") == [("run", "2", None, "2"), ("run", "3", None, "1")]


def test_given_a_logged_table_when_planning_then_the_plan_shows_the_logged_position(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT)
	assert "state   logged: no runs logged yet" in ok(project, "plan", "api__pages")
	give(project, "pages", [[{"n": 1}]])
	ok(project, "load", "api__pages")
	assert "state   logged: state 1 (run " in ok(project, "plan", "api__pages")


def test_given_the_run_log_insert_fails_when_loading_then_neither_the_data_nor_the_state_moves(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT)
	give(project, "pages", [[{"n": 1}]])
	ok(project, "load", "api__pages")
	execute(project, 'ALTER TABLE "RAW"."_sqlload_runs" RENAME COLUMN note TO note_moved')
	give(project, "pages", [[{"n": 1}], [{"n": 2}]])
	done = sqlload(project, "load", "api__pages")
	assert done.returncode == 1, done.stdout
	assert query(project, 'SELECT n FROM "RAW"."api__pages"') == [(1,)]
	execute(project, 'ALTER TABLE "RAW"."_sqlload_runs" RENAME COLUMN note_moved TO note')
	assert runs(project, "api__pages") == [("run", "1", None, "1")]
	ok(project, "load", "api__pages")
	assert seen(project, "pages")["state"] == 1
	assert query(project, 'SELECT n FROM "RAW"."api__pages" ORDER BY n') == [(1,), (2,)]


def test_given_a_generator_that_fails_when_loading_then_the_state_does_not_move(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT)
	give(project, "pages", [[{"n": 1}]])
	ok(project, "load", "api__pages")
	(project / "data" / "pages.json").write_text("not json")
	assert sqlload(project, "load", "api__pages").returncode == 1
	assert runs(project, "api__pages") == [("run", "1", None, "1")]


def test_given_the_run_log_is_deleted_when_loading_then_the_table_starts_over_as_a_stateless_one(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT)
	give(project, "pages", [[{"n": 1}]])
	ok(project, "load", "api__pages")
	execute(project, 'DROP TABLE "RAW"."_sqlload_runs"')
	ok(project, "load", "api__pages")
	assert seen(project, "pages")["state"] is None


def test_given_reload_when_loading_a_logged_table_then_it_reads_from_the_start_and_logs_the_new_position(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT)
	give(project, "pages", [[{"n": 1}], [{"n": 2}]])
	ok(project, "load", "api__pages")
	ok(project, "load", "--reload", "api__pages")
	assert seen(project, "pages") == {"state": None, "is_reload": True}
	assert query(project, 'SELECT n FROM "RAW"."api__pages" ORDER BY n') == [(1,), (2,)]
	assert runs(project, "api__pages")[-1] == ("run", "2", None, "2")


def test_given_a_reset_when_confirmed_then_the_next_run_reads_from_it(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT)
	give(project, "pages", [[{"n": 1}], [{"n": 2}], [{"n": 3}]])
	ok(project, "load", "api__pages")
	shown = ok(project, "state", "reset", "--to", "1", "api__pages")
	assert "would append  RAW.api__pages  reset" in shown and "add --yes" in shown
	assert len(runs(project, "api__pages")) == 1
	assert "appended  RAW.api__pages  reset" in ok(project, "state", "reset", "--to", "1", "--yes", "api__pages")
	ok(project, "load", "api__pages")
	assert seen(project, "pages")["state"] == 1
	assert [event for event, *_ in runs(project, "api__pages")] == ["run", "reset", "run"]
	ok(project, "state", "reset", "--clear", "--yes", "api__pages")
	ok(project, "load", "api__pages")
	assert seen(project, "pages")["state"] is None


def test_given_a_cursor_table_when_loading_then_runs_log_the_datas_position(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT)
	give(project, "events", [{"id": 1, "at": "2026-01-01T00:00:00Z"}, {"id": 2, "at": "2026-01-02T00:00:00Z"}])
	ok(project, "load", "api__events")
	give(project, "events", [{"id": 3, "at": "2026-01-03T00:00:00Z"}])
	ok(project, "load", "api__events")
	assert seen(project, "events")["cursor_value"] == "datetime.datetime(2026, 1, 2, 0, 0, tzinfo=datetime.timezone.utc)"
	assert [after for _, _, after, _ in runs(project, "api__events")] == ["2026-01-02 00:00:00.000000+00", "2026-01-03 00:00:00.000000+00"]


def test_given_a_log_ahead_of_the_data_when_verifying_and_reconciling_then_the_data_wins_visibly(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT)
	give(project, "events", [{"id": 1, "at": "2026-01-01T00:00:00Z"}, {"id": 2, "at": "2026-01-02T00:00:00Z"}])
	ok(project, "load", "api__events")
	execute(project, 'DELETE FROM "RAW"."api__events" WHERE id = 2')
	assert "the run log has at 2026-01-02 00:00:00.000000+00 but the data's maximum is 2026-01-01" in ok(project, "plan", "api__events")
	verified = sqlload(project, "state", "verify", "api__events")
	assert verified.returncode == 1
	assert "disagrees: the log has 2026-01-02 00:00:00.000000+00 but the data's maximum is 2026-01-01 00:00:00.000000+00" in verified.stderr
	assert "would append  RAW.api__events  reconcile" in ok(project, "state", "reconcile", "api__events")
	ok(project, "state", "reconcile", "--yes", "api__events")
	assert "ok" in ok(project, "state", "verify", "api__events")
	assert runs(project, "api__events")[-1] == ("reconcile", None, "2026-01-01 00:00:00.000000+00", "0")


def test_given_a_log_behind_the_data_when_loading_then_the_load_reconciles_first(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT)
	give(project, "events", [{"id": 1, "at": "2026-01-01T00:00:00Z"}])
	ok(project, "load", "api__events")
	execute(project, "INSERT INTO \"RAW\".\"api__events\" (id, \"at\") VALUES (2, '2026-01-05 00:00:00+00')")
	give(project, "events", [])
	ok(project, "load", "api__events")
	assert [event for event, *_ in runs(project, "api__events")] == ["run", "reconcile", "run"]
	assert runs(project, "api__events")[-1][2] == "2026-01-05 00:00:00.000000+00"


def test_given_a_backfill_when_loading_a_logged_table_then_it_is_logged_without_moving_the_state(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT)
	give(project, "events", [{"id": 1, "at": "2026-01-03T00:00:00Z"}])
	ok(project, "load", "api__events")
	give(project, "events", [{"id": 0, "at": "2026-01-01T00:00:00Z"}])
	ok(project, "load", "--start-cursor-ts", "2025-12-01T00:00:00Z", "--end-cursor-ts", "2026-01-02T00:00:00Z", "api__events")
	assert [event for event, *_ in runs(project, "api__events")] == ["run", "backfill"]
	assert "state   logged: at 2026-01-03 00:00:00.000000+00" in ok(project, "plan", "api__events")


def test_given_a_files_target_when_loading_twice_then_records_are_objects_and_the_second_run_resumes(tmp_path: Path) -> None:
	project = make_project(tmp_path, FILES_PROJECT)
	give(project, "pages", [[{"n": 1}], [{"n": 2}]])
	ok(project, "load", "api__pages")
	give(project, "pages", [[{"n": 1}], [{"n": 2}], [{"n": 3}]])
	ok(project, "load", "api__pages")
	assert seen(project, "pages")["state"] == 2
	assert file_values(project, "pages") == [1, 2, 3]
	records = file_runs(project, "api__pages")
	assert [(record["event"], record["state"], record["row_count"]) for record in records] == [("run", "2", 2), ("run", "3", 1)]
	assert all(record["files"] for record in records)


def test_given_a_files_load_that_stops_after_committing_when_loading_again_then_its_record_counts(tmp_path: Path) -> None:
	project = make_project(tmp_path, FILES_PROJECT)
	give(project, "pages", [[{"n": 1}]])
	ok(project, "load", "api__pages")
	records = project / "lake" / "_sqlload" / "runs" / "api__pages"
	records.chmod(0o500)
	give(project, "pages", [[{"n": 1}], [{"n": 2}]])
	failed = sqlload(project, "load", "api__pages")
	records.chmod(0o700)
	assert failed.returncode == 1, failed.stdout
	assert "state   logged: state 2" in ok(project, "plan", "api__pages")
	give(project, "pages", [[{"n": 1}], [{"n": 2}], [{"n": 3}]])
	ok(project, "load", "api__pages")
	assert seen(project, "pages")["state"] == 2
	assert file_values(project, "pages") == [1, 2, 3]
	assert [record["state"] for record in file_runs(project, "api__pages")] == ["1", "2", "3"]


def test_given_a_files_target_when_resetting_then_the_reset_is_an_object_and_the_next_run_reads_from_it(tmp_path: Path) -> None:
	project = make_project(tmp_path, FILES_PROJECT)
	give(project, "pages", [[{"n": 1}], [{"n": 2}]])
	ok(project, "load", "api__pages")
	ok(project, "state", "reset", "--to", "1", "--yes", "api__pages")
	ok(project, "load", "api__pages")
	assert seen(project, "pages")["state"] == 1
	assert [record["event"] for record in file_runs(project, "api__pages")] == ["run", "reset", "run"]


@pytest.mark.parametrize(
	("module", "message"),
	[
		("import sqlload as sl\n@sl.table(state='kept')\ndef t(ctx):\n    yield {}\n", "unknown variant `kept`"),
		(
			"import sqlload as sl\n@sl.table\ndef t(ctx):\n    ctx.state = 1\n    yield {'n': 1}\n",
			"ctx.state needs state='logged' on table t",
		),
		("import sqlload as sl\n@sl.table(state='logged')\ndef t(ctx):\n    ctx.state = object()\n    yield {'n': 1}\n", "ctx.state of table t is not JSON"),
	],
	ids=["bad value", "state without logged", "state not json"],
)
def test_given_bad_logged_state_use_when_loading_then_it_fails_clearly(tmp_path: Path, module: str, message: str) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT)
	(project / "sources" / "api.py").write_text(module)
	done = sqlload(project, "load")
	assert done.returncode == 1
	assert message in done.stdout + done.stderr, done.stdout + done.stderr


def test_given_no_logged_tables_when_showing_state_then_it_says_so(tmp_path: Path) -> None:
	project = make_project(tmp_path, DUCKDB_PROJECT)
	(project / "sources" / "api.py").write_text("import sqlload as sl\n@sl.table\ndef t(ctx):\n    yield {'n': 1}\n")
	assert 'no selected table has state = "logged"' in ok(project, "state", "show")
