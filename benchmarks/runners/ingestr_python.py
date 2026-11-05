"""Runs one Python-data scenario with ingestr's SDK (rows and Arrow over an IPC stream to its binary)."""

import json
import sys
from pathlib import Path

import ingestr

from sqlload_bench import pydata


def main() -> None:
	spec = json.loads(Path(sys.argv[1]).read_text())
	destination = {"dest_uri": f"duckdb:///{spec['destination']}", "dest_table": f"{spec['schema']}.{spec['table']}"}
	options = {"yes": True, "progress": "log", "pipelines_dir": str(Path(spec["run_dir"], "pipelines"))}
	if spec["scenario"] == "py_arrow":
		import pyarrow.parquet as pq

		ingestr.ingest(pq.read_table(Path(spec["data"], "python", "flat.parquet")), **destination, **options)
		return
	shape = spec["scenario"].removeprefix("py_")
	with ingestr.ingest(**destination, **options) as session:
		for page in pydata.pages(spec["rows"], spec["seed"], shape):
			session.ingest(page)


if __name__ == "__main__":
	main()
