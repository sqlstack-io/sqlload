"""Compare dlt's and SQLLoad's canonical datasets case by case, run by run; every difference must be allowed.

Usage: python -I tests/dlt_parity/compare.py <dlt.json> <sqlload.json> <allowlist.toml>
Prints each difference no entry allows, then each entry nothing matched, and exits 1 when there is either.

A difference is one of:
  load_failed     SQLLoad's load of the run failed (`detail` is its last line)
  missing_table   dlt has the table, SQLLoad does not
  extra_table     SQLLoad has the table, dlt does not
  missing_column  dlt's table has the column, SQLLoad's does not
  extra_column    SQLLoad's table has the column, dlt's does not
  column_type     both have the column, typed differently (`detail` is "dlt -> sqlload")
  row             the row at `place` differs, once columns only one side has are left out (a column typed
                  differently compares its values as text, so a type difference shows once, as column_type)
An allowlist entry names its `kind` and `reason`, and narrows it with `case`, `run`, `table`, `column` and
`place`, each the value itself or a glob (default "*"). A missing or extra row shows as a `row` difference with the other side empty.
"""

import json
import sys
import tomllib
from dataclasses import dataclass
from fnmatch import fnmatchcase
from typing import Any

KINDS = frozenset({"load_failed", "missing_table", "extra_table", "missing_column", "extra_column", "column_type", "row"})


@dataclass(frozen=True)
class Difference:
	kind: str
	case: str
	run: int
	table: str = ""
	column: str = ""
	place: str = ""
	detail: str = ""

	def __str__(self) -> str:
		where = " ".join(part for part in (f"table={self.table}" if self.table else "", f"column={self.column}" if self.column else "", f"place={self.place}" if self.place else "") if part)
		return f"{self.kind}  case={self.case} run={self.run} {where}  {self.detail}".rstrip()


def table_differences(case: str, run: int, table: str, dlt: dict[str, Any], sqlload: dict[str, Any]) -> list[Difference]:
	dlt_types = dict(map(tuple, dlt["columns"]))
	sqlload_types = dict(map(tuple, sqlload["columns"]))
	found = [Difference("missing_column", case, run, table, column) for column in dlt_types if column not in sqlload_types]
	found += [Difference("extra_column", case, run, table, column) for column in sqlload_types if column not in dlt_types]
	found += [
		Difference("column_type", case, run, table, column, detail=f"{dlt_types[column]} -> {sqlload_types[column]}")
		for column in dlt_types
		if column in sqlload_types and dlt_types[column] != sqlload_types[column]
	]
	shared = (set(dlt_types) & set(sqlload_types)) | {"place"}
	retyped = {column for column in shared if column in dlt_types and dlt_types[column] != sqlload_types[column]}
	dlt_rows = {row["place"]: comparable(row, shared, retyped) for row in dlt["rows"]}
	sqlload_rows = {row["place"]: comparable(row, shared, retyped) for row in sqlload["rows"]}
	for place in sorted(set(dlt_rows) | set(sqlload_rows)):
		if dlt_rows.get(place) != sqlload_rows.get(place):
			found.append(Difference("row", case, run, table, place=place, detail=f"{json.dumps(dlt_rows.get(place), sort_keys=True)} -> {json.dumps(sqlload_rows.get(place), sort_keys=True)}"))
	return found


def comparable(row: dict[str, Any], shared: set[str], retyped: set[str]) -> dict[str, Any]:
	"""`row`'s values of the `shared` columns, those of `retyped` columns as text."""
	return {name: str(value) if name in retyped and value is not None else value for name, value in row.items() if name in shared}


def failure(output: str) -> str:
	"""The reason a failed plan or table load gives, else the output's last line."""
	lines = [line.strip() for line in output.splitlines() if line.strip()]
	reasons = [line.split("reason", 1)[1].strip() for line in lines if line.lstrip("│└├─ ").startswith("reason ")]
	reasons += [line.split(None, 2)[-1] for line in lines if line.startswith("failed ")]
	return reasons[0] if reasons else (lines[-1] if lines else "")


def differences(dlt: dict[str, list[dict[str, Any]]], sqlload: dict[str, list[dict[str, Any]]]) -> list[Difference]:
	found: list[Difference] = []
	for case, dlt_runs in sorted(dlt.items()):
		sqlload_runs = sqlload.get(case, [])
		for run, dlt_tables in enumerate(dlt_runs, start=1):
			if run > len(sqlload_runs):
				break
			tables = sqlload_runs[run - 1]
			if "error" in tables:
				found.append(Difference("load_failed", case, run, detail=failure(tables["error"])))
				break
			found += [Difference("missing_table", case, run, table) for table in dlt_tables if table not in tables]
			found += [Difference("extra_table", case, run, table) for table in tables if table not in dlt_tables]
			for table in dlt_tables:
				if table in tables:
					found += table_differences(case, run, table, dlt_tables[table], tables[table])
	return found


def allows(entry: dict[str, str], difference: Difference) -> bool:
	"""Whether `entry` allows `difference`: its kind, and each of its globs equal to the value or matching it."""
	return entry["kind"] == difference.kind and all(
		(value := str(getattr(difference, name))) == (pattern := str(entry.get(name, "*"))) or fnmatchcase(value, pattern)
		for name in ("case", "run", "table", "column", "place")
	)


def main() -> None:
	dlt_path, sqlload_path, allowlist_path = sys.argv[1:4]
	with open(dlt_path) as dlt_file, open(sqlload_path) as sqlload_file, open(allowlist_path, "rb") as allowlist_file:
		dlt, sqlload, allowlist = json.load(dlt_file), json.load(sqlload_file), tomllib.load(allowlist_file)
	entries: list[dict[str, str]] = allowlist.get("allowed", [])
	problems = [f"allowlist entry {index}: unknown kind {entry.get('kind')!r}" for index, entry in enumerate(entries) if entry.get("kind") not in KINDS]
	problems += [f"allowlist entry {index}: it gives no reason" for index, entry in enumerate(entries) if not entry.get("reason", "").strip()]
	used = [False] * len(entries)
	unexplained: list[Difference] = []
	for difference in differences(dlt, sqlload):
		matched = [index for index, entry in enumerate(entries) if allows(entry, difference)]
		for index in matched:
			used[index] = True
		if not matched:
			unexplained.append(difference)
	problems += [f"not allowed: {difference}" for difference in unexplained]
	problems += [f"allowlist entry {index} matched nothing; remove it: {json.dumps(entry, sort_keys=True)}" for index, entry in enumerate(entries) if not used[index]]
	print("\n".join(problems) if problems else f"dlt parity: {len(dlt)} cases match, allowing {len(entries)} known differences")
	sys.exit(1 if problems else 0)


if __name__ == "__main__":
	main()
