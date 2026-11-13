# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = ["duckdb==2.0.0", "pytz==2026.6"]
# ///
"""Load every case with a SQLLoad binary into DuckDB under naming = "dlt", and print each run's canonical dataset as JSON.

A run whose load fails shows as {"error": <the load's output>}, and later runs of that case are skipped.
It reads the databases with the DuckDB SQLLoad bundles (2.0), which dlt's pinned DuckDB cannot open, so it runs as a
locked script of its own: uv run --locked --script tests/dlt_parity/run_sqlload.py <sqlload binary> <work folder>
"""

import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from canonical import dataset  # noqa: E402
from cases import CASES, Case  # noqa: E402

SCHEMA = "parity"
TABLE = "orders"


def project(case: Case, folder: Path) -> str:
	"""The case's sqlload.toml: its files in `drop/` loaded into `parity.orders` the way the case says."""
	strategy = {"append": "append", "replace": "replace", "merge": "merge"}[case.disposition]
	unnest = '"all"' if case.unnest == "all" else json.dumps(list(case.unnest))
	xml = '\nxml = { record_path = "orders/order" }' if case.fmt == "xml" else ""
	key = f'\nunique_key = ["{case.key}"]' if case.disposition == "merge" else ""
	return f"""[connections.wh]
kind = "duckdb"
database = "{folder / "out.duckdb"}"

[targets.dev]
connection = "wh"
loader_schema = "{SCHEMA}"

[connections.files]
kind = "filesystem"
root = "{folder}"

[sources.{TABLE}]
kind = "files"
description = "Parity case {case.name}."
connection = "files"
path = "drop/"
format = "{case.fmt}"{xml}
naming = "dlt"
write_strategy = "{strategy}"{key}

[sources.{TABLE}.tables.{TABLE}]
unnest = {unnest}
max_depth = {case.max_depth}
"""


def run_case(case: Case, binary: str, work: Path) -> list[dict[str, Any]]:
	folder = (work / case.name).resolve()
	shutil.rmtree(folder, ignore_errors=True)
	(folder / "drop").mkdir(parents=True, exist_ok=True)
	(folder / "sqlload.toml").write_text(project(case, folder))
	shown: list[dict[str, Any]] = []
	for number, text in enumerate(case.runs, start=1):
		if case.disposition == "replace":
			for old in (folder / "drop").iterdir():
				old.unlink()
		(folder / "drop" / f"run{number}.{case.fmt}").write_text(text)
		done = subprocess.run([binary, "load", "--config", str(folder / "sqlload.toml")], capture_output=True, text=True, check=False)
		if done.returncode != 0:
			shown.append({"error": (done.stdout + done.stderr).strip()})
			break
		shown.append(dataset(str(folder / "out.duckdb"), SCHEMA, TABLE, case.stored_key, case.disposition == "merge"))
	return shown


def main() -> None:
	binary, work = sys.argv[1], Path(sys.argv[2])
	work.mkdir(parents=True, exist_ok=True)
	print(json.dumps({case.name: run_case(case, binary, work) for case in CASES}, indent=1, sort_keys=True))


if __name__ == "__main__":
	main()
