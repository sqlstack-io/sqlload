"""A loaded DuckDB dataset in a form both tools' output can be compared in.

Ids are random for root rows outside a merge, so rows are named by where they sit instead:
a root row by its key, a child row by its parent's name, its table's last part and its position.
Deterministic ids (merge) are kept as they are, so they are compared too.
"""

from typing import Any

import duckdb

LINK_COLUMNS: frozenset[str] = frozenset({"_dlt_id", "_dlt_parent_id", "_dlt_list_idx", "_dlt_root_id"})
DROPPED_COLUMNS: frozenset[str] = frozenset({"_dlt_load_id", "_sqlload_loaded_at"})


def dataset(database: str, schema: str, root: str, key: str, keep_ids: bool) -> dict[str, Any]:
	"""Every table of `schema` named `root` or `root__*`: its columns and types, then its rows by place."""
	con = duckdb.connect(database, read_only=True)
	try:
		tables = [
			name
			for (name,) in con.execute(
				"SELECT table_name FROM information_schema.tables WHERE table_schema = ? ORDER BY length(table_name), table_name",
				[schema],
			).fetchall()
			if name == root or name.startswith(f"{root}__")
		]
		names: dict[str, str] = {}
		shown: dict[str, Any] = {}
		for table in tables:
			columns = con.execute(
				"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
				[schema, table],
			).fetchall()
			names_only = [name for name, _ in columns]
			rows = con.execute(f'SELECT * FROM "{schema}"."{table}"').fetchall()
			placed = []
			for row in rows:
				values = dict(zip(names_only, row, strict=True))
				if table == root:
					place = f"{key}={values.get(key)}"
				else:
					parent = names.get(str(values.get("_dlt_parent_id")), "?orphan")
					place = f"{parent}/{table.rsplit('__', 1)[-1]}[{values.get('_dlt_list_idx')}]"
				names[str(values.get("_dlt_id"))] = place
				data = {
					name: _plain(value)
					for name, value in values.items()
					if name not in DROPPED_COLUMNS and (name not in LINK_COLUMNS or (keep_ids and name in {"_dlt_id", "_dlt_root_id"}))
				}
				placed.append({"place": place, **data})
			placed.sort(key=lambda row: row["place"])
			shown[table] = {
				"columns": [[name, kind] for name, kind in columns if name not in DROPPED_COLUMNS],
				"rows": placed,
			}
		return shown
	finally:
		con.close()


def _plain(value: Any) -> Any:
	"""`value` as JSON can hold it."""
	if value is None or isinstance(value, bool | int | float | str):
		return value
	return str(value)
