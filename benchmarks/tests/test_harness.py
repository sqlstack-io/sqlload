"""The harness's own logic: data generation, checksums, measurement, reporting and the perf gate."""

import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from perf import compare
from sqlload_bench import datagen, pydata, snowflake
from sqlload_bench.competitors import HIDDEN, SQLLOAD, Context, SqlLoad, all_competitors
from sqlload_bench.measure import run
from sqlload_bench.report import Result, markdown
from sqlload_bench.scenarios import BY_NAME


def test_given_the_same_seed_when_generating_python_rows_then_they_are_identical_and_ids_run_from_one() -> None:
	first = [row for page in pydata.pages(2_500, 7, "flat") for row in page]
	again = [row for page in pydata.pages(2_500, 7, "flat") for row in page]
	other = [row for page in pydata.pages(2_500, 8, "flat") for row in page]
	assert first == again
	assert first != other
	assert [row["id"] for row in first] == list(range(1, 2_501))
	assert len(first[0]) == 20


def test_given_nested_rows_when_counting_lines_then_the_count_matches_the_rows() -> None:
	rows = [row for page in pydata.pages(300, 3, "nested") for row in page]
	assert sum(len(row["lines"]) for row in rows) == pydata.nested_lines(300, 3)


@pytest.mark.parametrize("chunk_bytes", [7, 64, 1 << 20])
def test_given_any_chunk_size_when_splitting_ndjson_then_chunks_end_at_line_ends_and_keep_every_line(tmp_path: Path, chunk_bytes: int) -> None:
	path = tmp_path / "rows.ndjson"
	path.write_bytes(b"".join(b'{"id":%d}\n' % index for index in range(100)) + b'{"id":100}')
	chunks = list(pydata.ndjson_chunks(str(path), chunk_bytes))
	assert all(chunk.endswith(b"\n") for chunk in chunks[:-1])
	assert b"".join(chunks) == path.read_bytes()


def test_given_generated_data_when_checksumming_each_format_then_parquet_csv_and_ndjson_agree(tmp_path: Path) -> None:
	dataset = datagen.generate(tmp_path, 5, datagen.SIZES["tiny"])
	expected = dataset.expected("files")
	with duckdb.connect() as connection:
		connection.execute("SET TimeZone = 'UTC'")
		for source in (
			f"read_csv('{dataset.path('files/csv')}/*.csv')",
			f"read_json('{dataset.path('files/ndjson')}/*.ndjson')",
			f"read_csv('{dataset.path('files/single/narrow.csv')}')",
			f"'{dataset.path('narrow.parquet')}'",
		):
			assert datagen.checksum(connection, "narrow", source) == expected, source
	assert dataset.expected("narrow_incremental")[0] == str(dataset.sizes.narrow + dataset.sizes.append)
	assert dataset.first_appended_at() > "2026-01-01T00:00:00Z"


def test_given_a_process_tree_when_measured_then_cpu_of_children_and_peak_memory_are_counted(tmp_path: Path) -> None:
	script = "import subprocess, sys; subprocess.run([sys.executable, '-c', 'x = bytearray(200 << 20); sum(range(3_000_000))'])"
	done = run([sys.executable, "-c", script], cwd=tmp_path, env={}, log=tmp_path / "log")
	assert done.returncode == 0
	assert done.cpu_s > 0.05
	assert done.peak_rss_mb > 150


def test_given_results_when_reporting_then_each_row_shows_its_ratio_to_sqlload() -> None:
	runs = [{"valid": True, "wall_s": wall, "cpu_s": 1.0, "user_s": 1.0, "sys_s": 0.0, "peak_rss_mb": 100.0} for wall in (2.0, 3.0, 4.0)]
	sqlload = Result("pg_narrow", "sqlload", 1000, runs=runs)
	slower = Result("pg_narrow", "dlt (pyarrow)", 1000, runs=[{**run, "wall_s": run["wall_s"] * 2} for run in runs])
	missing = Result("py_bytes", "dlt", 1000, status="n/a", note="no bytes input")
	table = markdown([sqlload.summary(), slower.summary(), missing.summary()], "## t")
	assert "| pg_narrow | sqlload | 1,000 | 3.00 |" in table
	assert "| 2.00x |" in table
	assert "n/a: no bytes input" in table


def _snowflake_env(**extra: str) -> dict[str, str]:
	base = {f"{snowflake.PREFIX}{name}": name.lower() for name in snowflake.REQUIRED}
	return {**base, snowflake.TOKEN_ENV: "secret", **extra}


FILES_ENV = {
	"SQLLOAD_BENCH_S3_BUCKET": "example-bucket-test",
	"SQLLOAD_BENCH_S3_REGION": "eu-west-2",
	"AWS_ACCESS_KEY_ID": "test",
	"AWS_SECRET_ACCESS_KEY": "test",
	f"{snowflake.PREFIX}STORAGE_INTEGRATION": "LAKE_INT",
}


def test_given_partial_settings_when_checking_a_snowflake_scenario_then_each_missing_variable_is_named() -> None:
	assert snowflake.missing_settings("pg_snowflake", _snowflake_env()) == []
	assert f"{snowflake.PREFIX}ACCOUNT" in snowflake.missing_settings("pg_snowflake", {})
	assert "SQLLOAD_BENCH_S3_BUCKET" in snowflake.missing_settings("files_snowflake", _snowflake_env())
	assert snowflake.missing_settings("files_snowflake", _snowflake_env(**FILES_ENV)) == []


def test_given_a_dataset_when_naming_its_s3_folder_then_it_stays_under_the_sqlload_prefix(tmp_path: Path) -> None:
	dataset = datagen.Dataset(tmp_path / "seed1", 1, datagen.SIZES["tiny"])
	assert snowflake.files_key(dataset) == "sqlload/bench/seed1/parquet/"


def test_given_a_table_of_someone_else_when_dropping_then_the_harness_refuses() -> None:
	with pytest.raises(ValueError, match="BENCH_"):
		snowflake.drop_table(snowflake.settings_from_env(_snowflake_env()), "ORDERS")


def test_given_equal_values_in_different_text_forms_when_comparing_then_they_match() -> None:
	assert snowflake.same_numbers([Decimal("947612460.6"), 3], ["947612460.60", "3"])
	assert not snowflake.same_numbers([Decimal("947612460.7"), 3], ["947612460.60", "3"])
	assert not snowflake.same_numbers([1], ["1", "2"])


@pytest.mark.parametrize(
	("competitor", "expected_text"),
	[
		(SqlLoad(direct=True, label="direct"), "destination loads parquet files from"),
	],
	ids=["direct"],
)
@pytest.mark.skipif(not SQLLOAD.exists(), reason="the sqlload wheel is not installed")
def test_given_the_files_to_snowflake_project_when_validated_then_sqlload_accepts_it_and_the_route_is_checked(
	tmp_path: Path, competitor: SqlLoad, expected_text: str
) -> None:
	settings = snowflake.settings_from_env(_snowflake_env(**FILES_ENV))
	dataset = datagen.Dataset(tmp_path / "seed1", 1, datagen.SIZES["tiny"])
	context = Context(dataset, None, tmp_path, settings)
	plan, text = competitor.plan_invocation(BY_NAME["files_snowflake"], context, tmp_path) or (None, "")
	assert plan is not None and text == expected_text
	assert ("sqlload/bench/seed1/parquet/" in (plan.cwd / "sqlload.toml").read_text())
	done = subprocess.run(
		[str(SQLLOAD), "debug", "--no-connection"], cwd=plan.cwd, env={**plan.env, snowflake.TOKEN_ENV: "secret"}, capture_output=True, text=True, check=False
	)
	assert done.returncode == 0, done.stdout + done.stderr


def test_given_the_sizes_when_reading_the_multi_table_scenario_then_it_holds_all_narrow_rows() -> None:
	assert BY_NAME["pg_multi"].rows(datagen.SIZES["tiny"]) == datagen.SIZES["tiny"].narrow


def test_given_each_loader_when_asked_about_scenarios_then_python_is_dlt_only_and_threads_only_multi() -> None:
	by_name = {competitor.name: competitor for competitor in all_competitors()}
	assert by_name["ingestr"].unsupported(BY_NAME["py_flat"]) == HIDDEN
	assert by_name["dlt"].unsupported(BY_NAME["py_bytes"]) is None
	assert by_name["sqlload (--threads 1)"].unsupported(BY_NAME["pg_narrow"]) == HIDDEN
	assert by_name["sqlload (--threads 1)"].unsupported(BY_NAME["pg_multi"]) is None
	assert by_name["sqlload"].unsupported(BY_NAME["files_snowflake"]) == HIDDEN


def _summary_line(name: str, bench_id: str, ir: int) -> str:
	return json.dumps(
		{
			"module_path": f"hot_paths::{name}",
			"id": bench_id,
			"profiles": [{"tool": "Callgrind", "data": {"total": {"metrics": {"Ir": {"values": {"new": ir}}}}}}],
		}
	)


@pytest.mark.parametrize(
	("head_ir", "accept", "expected_exit", "expected_text"),
	[
		(1_049, False, 0, "No benchmark regressed"),
		(1_051, False, 1, "regressed by more than 5%"),
		(1_051, True, 0, "accepted by the `perf-regression-accepted` label"),
		(900, False, 0, "improved"),
	],
	ids=["within", "regressed", "accepted", "improved"],
)
def test_given_base_and_head_counts_when_comparing_then_only_growth_beyond_the_threshold_fails(
	tmp_path: Path, head_ir: int, accept: bool, expected_exit: int, expected_text: str
) -> None:
	base, head, summary = tmp_path / "base.jsonl", tmp_path / "head.jsonl", tmp_path / "summary.md"
	base.write_text(_summary_line("g::decode", "narrow", 1_000) + "\nnot json\n" + _summary_line("g::gone", "x", 5) + "\n")
	head.write_text(_summary_line("g::decode", "narrow", head_ir) + "\n" + _summary_line("g::added", "y", 7) + "\n")
	argv = ["--base", str(base), "--head", str(head), "--summary", str(summary), *(["--accept"] if accept else [])]
	assert compare.main(argv) == expected_exit
	text = summary.read_text()
	assert expected_text in text
	assert "| `g::added y` |  | 7 | | new |" in text
	assert "| `g::gone x` | 5 |  | | removed |" in text


def test_given_no_base_results_when_comparing_then_it_reports_nothing_to_compare_and_passes(tmp_path: Path) -> None:
	head = tmp_path / "head.jsonl"
	head.write_text(_summary_line("g::decode", "narrow", 10) + "\n")
	assert compare.main(["--base", str(tmp_path / "missing.jsonl"), "--head", str(head)]) == 0
