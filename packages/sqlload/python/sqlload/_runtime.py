"""Discovering Python sources under `python_paths` and calling their table functions for the loader.

The native loader calls these functions through `sqlload._native` while running `sqlload plan`,
`load` and the other commands; they are not a public API.
"""

import importlib
import json
import logging
import sys
import traceback
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlload._declare import Connection, Context, Source, Table

_tables: dict[str, Table] = {}
_clients: dict[str, Any] = {}


class DiscoveryError(Exception):
	"""A module under `python_paths` cannot be imported, or its declarations clash."""


def discover(root: str, python_paths: list[str]) -> str:
	"""Import every module under `python_paths` (relative to `root`) and describe the sources found, as JSON."""
	root_path = Path(root or ".").resolve()
	if str(root_path) not in sys.path:
		sys.path.insert(0, str(root_path))
	sources: dict[int, tuple[Source, str]] = {}
	standalone: dict[int, tuple[Table, str]] = {}
	for module_path in _module_paths(root_path, python_paths):
		relative = module_path.relative_to(root_path).as_posix()
		module = _import(root_path, module_path, relative)
		for value in vars(module).values():
			if isinstance(value, Source):
				sources.setdefault(id(value), (value, relative))
			elif isinstance(value, Table) and value.source is None:
				standalone.setdefault(id(value), (value, relative))
			elif isinstance(value, Table) and value.source is not None:
				sources.setdefault(id(value.source), (value.source, relative))
	_tables.clear()
	declared: list[dict[str, Any]] = []
	for source, relative in sources.values():
		declared.append(_source_declaration(source, relative))
	for table, relative in standalone.values():
		declared.append(
			{"name": table.name, "module": relative, "settings": {"tables": {table.name: _table_settings(table)}}}
		)
	for source, _ in sources.values():
		for table in source.tables:
			_remember(table)
	for table, _ in standalone.values():
		_remember(table)
	return json.dumps(declared)


def start_table(request: str) -> Iterator[Any]:
	"""Call a table function with its context; returns what it yields."""
	read = json.loads(request)
	table = _tables[read["table"]]
	connections: dict[str, Any] = read["connections"]
	connection_name = read.get("connection")
	connection = Connection(connections.get(connection_name, {}) if connection_name else {}, connections)
	cursor = read.get("cursor") or {}
	source = table.source
	ctx = Context(
		table=table.name,
		source=read["source"],
		connection=connection,
		logger=logging.getLogger(f"sqlload.{read['source']}.{table.name}"),
		cursor_column=cursor.get("column"),
		cursor_value=_cursor_value(cursor.get("start"), cursor.get("column_type")),
		cursor_end=_cursor_value(cursor.get("end"), cursor.get("column_type")),
		cursor_inclusive=cursor.get("inclusive", True),
		is_reload=read["is_reload"],
		client_factory=_shared_client(source) if source is not None and source.client_factory else None,
	)
	yielded = table.function(ctx)
	return iter(()) if yielded is None else iter(yielded)


def finish_run() -> None:
	"""Close every client the run built (their `close()`, when they have one)."""
	clients = list(_clients.values())
	_clients.clear()
	for client in clients:
		close = getattr(client, "close", None)
		if callable(close):
			close()


def _module_paths(root: Path, python_paths: list[str]) -> list[Path]:
	paths: list[Path] = []
	for entry in python_paths:
		folder = (root / entry).resolve()
		if not folder.is_dir():
			raise DiscoveryError(f"python_paths entry {entry!r} is not a folder under {root}")
		for path in sorted(folder.rglob("*.py")):
			parts = path.relative_to(root).parts
			if any(part.startswith(".") or part == "__pycache__" for part in parts) or path.name.startswith("_"):
				continue
			if path not in paths:
				paths.append(path)
	return paths


def _import(root: Path, module_path: Path, relative: str) -> Any:
	name = ".".join(module_path.relative_to(root).with_suffix("").parts)
	try:
		return importlib.import_module(name)
	except Exception as error:
		frames = traceback.extract_tb(error.__traceback__)
		place = next(
			(f" (line {frame.lineno} of {Path(frame.filename).name})" for frame in reversed(frames) if frame.filename.startswith(str(root))),
			"",
		)
		raise DiscoveryError(f"cannot import {relative}: {type(error).__name__}: {error}{place}") from error


def _remember(table: Table) -> None:
	_tables[table.name] = table


def _source_declaration(source: Source, relative: str) -> dict[str, Any]:
	settings: dict[str, Any] = {key: _setting(value) for key, value in source.settings.items() if value is not None}
	if source.description is not None:
		settings["description"] = source.description
	settings["tables"] = {table.name: _table_settings(table) for table in source.tables}
	return {"name": source.name, "module": relative, "settings": settings}


def _table_settings(table: Table) -> dict[str, Any]:
	settings: dict[str, Any] = {key: _setting(value) for key, value in table.settings.items() if value is not None}
	if table.description is not None:
		settings["description"] = table.description
	return settings


def _setting(value: Any) -> Any:
	"""A setting as `sqlload.toml` writes it: durations as `"90s"`, tuples as lists."""
	if isinstance(value, timedelta):
		return f"{int(value.total_seconds())}s"
	if isinstance(value, (date, datetime)):
		return value.isoformat()
	if isinstance(value, (tuple, list)):
		return [_setting(item) for item in value]
	if isinstance(value, dict):
		return {str(key): _setting(item) for key, item in value.items() if item is not None}
	return value


def _shared_client(source: Source) -> Any:
	def build(ctx: Context) -> Any:
		if source.name not in _clients:
			assert source.client_factory is not None
			_clients[source.name] = source.client_factory(ctx)
		return _clients[source.name]

	return build


def _cursor_value(text: str | None, column_type: str | None) -> Any:
	"""A cursor bound in its Python type: int, float, date, or datetime (aware for `timestamp_tz`)."""
	if text is None:
		return None
	try:
		if column_type in ("int", "smallint"):
			return int(text)
		if column_type == "float":
			return float(text)
		if column_type == "date":
			return date.fromisoformat(text[:10])
		if column_type in ("timestamp_tz", "timestamp_ntz"):
			value = datetime.fromisoformat(text.removesuffix("+00").replace("Z", ""))
			return value.replace(tzinfo=timezone.utc) if column_type == "timestamp_tz" else value
	except ValueError:
		return text
	return text
