"""`python -m sqlload_bench`: generate the data, run every loader on every scenario, check and report."""

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from sqlload_bench import datagen, postgres, snowflake
from sqlload_bench.competitors import BENCH_ROOT, DLT_PYTHON, HIDDEN, INGESTR, SQLLOAD, Competitor, Context, Dlt, all_competitors
from sqlload_bench.measure import run as measure
from sqlload_bench.report import Result, markdown
from sqlload_bench.scenarios import SCENARIOS, Scenario


def check(competitor: Competitor, scenario: Scenario, context: Context, run_dir: Path) -> tuple[bool, str]:
	"""Whether the destination holds exactly the source's rows (count and checksum over key columns)."""
	expected = context.dataset.expected(scenario.expected)
	if scenario.group == "snowflake":
		assert context.warehouse is not None
		return snowflake.check(context.warehouse, scenario.table, expected)
	try:
		with duckdb.connect(str(competitor.destination(run_dir)), read_only=True) as connection:
			connection.execute("SET TimeZone = 'UTC'")
			got = datagen.checksum(connection, scenario.kind, competitor.relation(scenario))
			child = competitor.child_table(scenario) if isinstance(competitor, Dlt) else None
			if child:
				lines = connection.execute(f"SELECT count(*) FROM {child}").fetchone()
				if [str(lines[0] if lines else 0)] != context.dataset.expected("nested_lines"):
					return False, f"{child} has {lines} rows, expected {context.dataset.expected('nested_lines')}"
	except duckdb.Error as error:
		return False, f"check failed: {error}"
	if got != expected:
		return False, f"checksum {got} != expected {expected}"
	return True, ""


def one_run(competitor: Competitor, scenario: Scenario, context: Context, run_dir: Path) -> dict[str, Any]:
	"""One fresh destination; for the incremental scenario, the second load is the measured one."""
	shutil.rmtree(run_dir, ignore_errors=True)
	run_dir.mkdir(parents=True)
	record: dict[str, Any] = {"load_avg_1m": os.getloadavg()[0]}
	if scenario.group == "snowflake":
		assert context.warehouse is not None
		snowflake.drop_table(context.warehouse, scenario.table)
	planned = competitor.plan_invocation(scenario, context, run_dir)
	if planned is not None:
		invocation, expected_text = planned
		done = measure(invocation.argv, cwd=invocation.cwd, env=invocation.env, log=run_dir / "plan.log")
		if done.returncode != 0 or expected_text not in done.output:
			return {**record, "valid": False, "returncode": done.returncode, "error": f"the plan does not say {expected_text!r}: {done.output[-1500:]}"}
	incremental = scenario.name == "pg_incremental" and context.postgres is not None
	try:
		if incremental:
			first = competitor.invocation(scenario, context, run_dir)
			done = measure(first.argv, cwd=first.cwd, env=first.env, log=run_dir / "first.log")
			record["first_load_wall_s"] = done.wall_s
			if done.returncode != 0:
				return {**record, "valid": False, "returncode": done.returncode, "error": f"first load exited {done.returncode}: {done.output[-1500:]}"}
			postgres.append_rows(context.postgres, context.dataset)
		invocation = competitor.invocation(scenario, context, run_dir, second=incremental)
		done = measure(invocation.argv, cwd=invocation.cwd, env=invocation.env, log=run_dir / "load.log")
		record.update(
			wall_s=done.wall_s, user_s=done.user_s, sys_s=done.sys_s, cpu_s=done.cpu_s, peak_rss_mb=done.peak_rss_mb, returncode=done.returncode
		)
		if done.returncode != 0:
			record.update(valid=False, error=f"exited {done.returncode}: {done.output[-1500:]}")
		else:
			valid, reason = check(competitor, scenario, context, run_dir)
			record.update(valid=valid, error=reason)
		return record
	finally:
		if incremental:
			postgres.remove_appended(context.postgres, context.dataset)


def versions() -> dict[str, str]:
	def output(argv: list[str]) -> str:
		try:
			return subprocess.run(argv, capture_output=True, text=True, timeout=120, check=False).stdout.strip().splitlines()[-1]
		except (OSError, IndexError, subprocess.TimeoutExpired):
			return "unavailable"

	git = subprocess.run(["git", "-C", str(BENCH_ROOT), "rev-parse", "HEAD"], capture_output=True, text=True, check=False).stdout.strip()
	return {
		"sqlload": output([str(SQLLOAD), "--version"]),
		"sqlload_commit": git,
		"dlt": output([str(DLT_PYTHON), "-c", "import dlt, duckdb; print(dlt.__version__, 'duckdb', duckdb.__version__)"]),
		"ingestr": output([str(INGESTR), "--version"]),
		"harness_duckdb": duckdb.__version__,
		"python": platform.python_version(),
		"postgres_image": postgres.IMAGE,
	}


def machine() -> dict[str, Any]:
	cpu = ""
	try:
		cpu = next(line.split(":", 1)[1].strip() for line in open("/proc/cpuinfo") if line.startswith("model name"))
	except (OSError, StopIteration):
		pass
	return {"cpu": cpu, "cores": os.cpu_count(), "platform": platform.platform(), "load_avg": os.getloadavg()}


def load_range(results: list[Result]) -> str:
	loads = [run["load_avg_1m"] for result in results for run in result.runs if "load_avg_1m" in run]
	return f"{min(loads):.1f} to {max(loads):.1f}" if loads else "not measured"


def parse_args(argv: list[str]) -> argparse.Namespace:
	parser = argparse.ArgumentParser(prog="sqlload_bench", description=__doc__)
	parser.add_argument("--out", type=Path, required=True, help="where results.json and results.md go")
	parser.add_argument("--data", type=Path, help="where generated data is cached (default: OUT/../data)")
	parser.add_argument("--size", choices=sorted(datagen.SIZES), default="full")
	parser.add_argument("--narrow-rows", type=int, help="override the narrow table's (and the files') row count")
	parser.add_argument("--seed", type=int, default=42)
	parser.add_argument("--runs", type=int, default=3)
	parser.add_argument("--warmup", type=int, default=1)
	parser.add_argument("--scenarios", default="", help="comma-separated scenario names or groups (default: all)")
	parser.add_argument("--competitors", default="", help="comma-separated loader names (default: all)")
	parser.add_argument("--work", type=Path, help="scratch folder for destinations (default: OUT/work)")
	parser.add_argument("--wait-for-builds", action="store_true", help="before each run, wait until no compiler is running on this machine")
	return parser.parse_args(argv)


def wait_for_builds() -> None:
	"""On a shared machine, start a run only while nothing is compiling."""
	while subprocess.run(["pgrep", "-x", "rustc|cc1plus|cc"], capture_output=True, check=False).returncode == 0:
		time.sleep(10)


def selected(values: str, name: str, group: str = "") -> bool:
	wanted = {value.strip() for value in values.split(",") if value.strip()}
	return not wanted or name in wanted or group in wanted


def main(argv: list[str] | None = None) -> int:
	args = parse_args(sys.argv[1:] if argv is None else argv)
	sizes = datagen.SIZES[args.size]
	if args.narrow_rows:
		sizes = datagen.Sizes(**{**sizes.__dict__, "narrow": args.narrow_rows, "files": args.narrow_rows})
	args.out.mkdir(parents=True, exist_ok=True)
	work = args.work or args.out / "work"
	scenarios = [s for s in SCENARIOS if selected(args.scenarios, s.name, s.group)]
	competitors = [c for c in all_competitors() if selected(args.competitors, c.name)]
	print(f"generating data ({args.size}, seed {args.seed})", flush=True)
	dataset = datagen.generate(args.data or args.out / "data", args.seed, sizes)
	# Snowflake scenarios run only when asked for by name or group, with their variables set.
	explicit = {value.strip() for value in args.scenarios.split(",")}
	snowflake_settings: snowflake.Settings | None = None
	blocked: dict[str, str] = {}
	for scenario in [s for s in scenarios if s.group == "snowflake"]:
		missing = snowflake.missing_settings(scenario.name)
		if not ({scenario.name, scenario.group} & explicit):
			blocked[scenario.name] = "only runs when selected with --scenarios"
		elif missing:
			blocked[scenario.name] = "set " + ", ".join(missing)
		else:
			snowflake_settings = snowflake.settings_from_env()
			if scenario.name == "files_snowflake":
				print(f"test data: s3://{snowflake_settings.bucket}/{snowflake.upload_parquet(snowflake_settings, dataset)}", flush=True)
	active_names = {s.name for s in scenarios} - blocked.keys()
	needs_postgres = any(s.group == "postgres" or s.name == "pg_snowflake" for s in scenarios if s.name in active_names)
	pg = postgres.start() if needs_postgres else None
	started = datetime.now(UTC)
	results: list[Result] = []
	try:
		if pg is not None:
			print(f"loading Postgres on port {pg.port}", flush=True)
			postgres.load(pg, dataset)
		context = Context(dataset, pg, args.out / "cache", snowflake_settings)
		for scenario in scenarios:
			active: list[tuple[Competitor, Result]] = []
			for competitor in competitors:
				result = Result(scenario.name, competitor.name, scenario.rows(sizes))
				reason = competitor.unsupported(scenario)
				if reason == HIDDEN:
					continue
				reason = reason or blocked.get(scenario.name)
				if reason:
					result.status, result.note = ("skipped" if scenario.pending or scenario.name in blocked else "n/a"), reason
					if competitor.name == "sqlload" or not scenario.pending:
						results.append(result)
					continue
				results.append(result)
				active.append((competitor, result))
			# Warm-ups first, then measured runs round-robin so drift in machine load spreads over every loader.
			for attempt in range(args.warmup + args.runs):
				for competitor, result in active:
					if result.status == "failed":
						continue
					run_dir = work / scenario.name / competitor.name.replace(" ", "_").replace("(", "").replace(")", "")
					if args.wait_for_builds:
						wait_for_builds()
					print(f"{scenario.name:18} {competitor.name:24} {'warm-up' if attempt < args.warmup else f'run {attempt - args.warmup + 1}'}", flush=True)
					record = one_run(competitor, scenario, context, run_dir)
					record["warmup"] = attempt < args.warmup
					print(f"  {record.get('wall_s', 0):8.2f}s valid={record['valid']} {record.get('error', '')[:300]}", flush=True)
					if record["warmup"]:
						if not record["valid"]:
							result.status, result.note = ("invalid" if record.get("returncode") == 0 else "failed"), record["error"][:300]
							result.runs.append(record)
						continue
					result.runs.append(record)
					if not record["valid"]:
						result.status, result.note = "invalid", record["error"][:300]
			shutil.rmtree(work / scenario.name, ignore_errors=True)
	finally:
		if pg is not None:
			pg.stop()
	summaries = [result.summary() for result in results]
	header = (
		f"## SQLLoad benchmarks ({args.size}, seed {args.seed}, median of {args.runs} after {args.warmup} warm-up)\n\n"
		f"Started {started:%Y-%m-%d %H:%M} UTC on {machine()['cpu']} ({os.cpu_count()} cores); 1-minute load before each run: {load_range(results)}."
	)
	document = {"started": started.isoformat(), "finished": datetime.now(UTC).isoformat(), "size": args.size, "sizes": sizes.__dict__,
		"seed": args.seed, "runs": args.runs, "warmup": args.warmup, "versions": versions(), "machine": machine(), "results": summaries}
	(args.out / "results.json").write_text(json.dumps(document, indent=1, default=str))
	with (args.out / "history.jsonl").open("a") as history:
		# One line per suite run, so improvement over time stays visible after results.json is replaced.
		kept = ("scenario", "competitor", "rows", "status", "wall_s", "cpu_s", "peak_rss_mb", "rows_per_s")
		line = {key: document[key] for key in ("started", "size", "seed", "versions")}
		history.write(json.dumps({**line, "results": [{key: s[key] for key in kept} for s in summaries]}, default=str) + "\n")
	table = markdown(summaries, header)
	(args.out / "results.md").write_text(table)
	print(table)
	return 0 if all(s["status"] in {"ok", "n/a", "skipped"} for s in summaries) else 1


if __name__ == "__main__":
	time.tzset()
	sys.exit(main())
