"""Produces a Python scenario's data and discards it: the cost every loader pays before loading."""

import json
import sys
from pathlib import Path

from sqlload_bench import pydata


def main() -> None:
	spec = json.loads(Path(sys.argv[1]).read_text())
	scenario, data = spec["scenario"], Path(spec["data"], "python")
	if scenario == "py_arrow":
		import pyarrow.parquet as pq

		count = pq.read_table(data / "flat.parquet").num_rows
	elif scenario == "py_bytes":
		count = sum(chunk.count(b"\n") for chunk in pydata.ndjson_chunks(str(data / "flat.ndjson")))
	else:
		count = sum(len(page) for page in pydata.pages(spec["rows"], spec["seed"], scenario.removeprefix("py_")))
	print(count)


if __name__ == "__main__":
	main()
