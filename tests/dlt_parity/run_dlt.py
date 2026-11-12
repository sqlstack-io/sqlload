"""Load every case with the pinned dlt into DuckDB, and print each run's canonical dataset as JSON.

Usage: uv run --project tests/dlt_parity python -I tests/dlt_parity/run_dlt.py <work folder>
"""

import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

import dlt  # noqa: E402
import xmltodict  # noqa: E402

from canonical import dataset  # noqa: E402
from cases import CASES, Case  # noqa: E402

SCHEMA = "parity"
TABLE = "orders"


def rows(case: Case, text: str) -> list[dict[str, Any]]:
	"""The rows of one run's file, as a dlt user reads them: NDJSON line by line, XML with xmltodict."""
	if case.fmt == "ndjson":
		return [json.loads(line) for line in text.splitlines() if line.strip()]
	orders = xmltodict.parse(text)["orders"]["order"]
	return orders if isinstance(orders, list) else [orders]


def disposition(case: Case) -> Any:
	return {"disposition": "merge", "strategy": "upsert"} if case.disposition == "merge" else case.disposition


def run_case(case: Case, work: Path) -> list[dict[str, Any]]:
	database = str(work / f"{case.name}.duckdb")
	pipeline = dlt.pipeline(
		pipeline_name=f"parity_{case.name}",
		pipelines_dir=str(work / "pipelines"),
		destination=dlt.destinations.duckdb(database),
		dataset_name=SCHEMA,
	)
	shown = []
	for text in case.runs:
		resource = dlt.resource(
			rows(case, text),
			name=TABLE,
			write_disposition=disposition(case),
			primary_key=case.key if case.disposition == "merge" else None,
			max_table_nesting=case.max_depth,
		)
		pipeline.run(resource)
		shown.append(dataset(database, SCHEMA, TABLE, case.key, case.disposition == "merge"))
	return shown


def main() -> None:
	work = Path(sys.argv[1])
	work.mkdir(parents=True, exist_ok=True)
	print(json.dumps({case.name: run_case(case, work) for case in CASES}, indent=1, sort_keys=True))


if __name__ == "__main__":
	main()
