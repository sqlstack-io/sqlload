"""Discovering Python sources under `python_paths` and calling their table functions for the loader.

The native loader calls these functions through `sqlload._native` while running `sqlload plan`,
`load` and the other commands; they are not a public API.
"""

import importlib
import json
import sys
import threading
import traceback
from collections.abc import Iterator
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlload._declare import Context, Source, Table
from sqlload._rename import RENAMES, Column, RenameContext, renamed
from sqlload_api import _host
from sqlload_api._http import Job
from sqlload_api._http_paths import literal_paths

#: Folders under `python_paths` never imported: virtual environments, build output and caches.
SKIPPED_FOLDERS: frozenset[str] = frozenset({"venv", "build", "dist", "site-packages", "node_modules"})

#: The destination name of a table declared without a source: its own name.
STANDALONE_TABLE_NAME = "{table}"

_tables: dict[str, Table] = {}
#: The project folder `discover` imported from, which errors name files relative to.
_root: list[Path] = []
_contexts: dict[str, Context] = {}
_clients: dict[str, Any] = {}
#: Held while a client is built, since tables of one source load at the same time.
_clients_lock = threading.Lock()


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
	_root[:] = [root_path]
	_host.set_root(root_path)
	_tables.clear()
	declared: list[dict[str, Any]] = []
	for source, relative in sources.values():
		declared.append(_source_declaration(source, relative))
	for table, relative in standalone.values():
		declared.append(
			{
				"name": table.name,
				"module": relative,
				"settings": {"table_name": STANDALONE_TABLE_NAME, "tables": {table.name: _table_settings(table)}},
				"http_paths": _http_paths([table]),
			}
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
	source = table.source
	shared = _shared_client(source) if source is not None and source.client_factory else None
	ctx = Context.from_read(read, client_factory=shared)
	_contexts[table.name] = ctx
	yielded = table.function(ctx)
	if yielded is None:
		return iter(())
	return iter([yielded]) if isinstance(yielded, Job) else iter(yielded)


def current_state(table: str) -> str | None:
	"""The JSON `ctx.state` was assigned one assignment before its latest, while the table function runs (see `sqlload_api._host.current_state`)."""
	return _host.current_state(_contexts.get(table))


def table_state(table: str) -> str | None:
	"""The position the table function left in `ctx.state`, as JSON, when it differs from the one it started with (see `sqlload_api._host.final_state`)."""
	return _host.final_state(_contexts.pop(table, None))


def rename(name: str, columns: str | None, context: str) -> tuple[str | None, str | None]:
	"""Call the rename function `name` on `columns` (JSON: each column's `position`, `source` and `default`) with its
	`ctx` (JSON); with no `columns`, only look it up.

	Returns the names it gives, by position, as JSON, or why it gives none: no function or two are registered as
	`name`, or it failed or returned something else.
	"""
	function, problem = RENAMES.lookup(name, _root[0] if _root else None)
	if function is None:
		return None, problem
	if columns is None:
		return "{}", None
	given = [Column(**column) for column in json.loads(columns)]
	ctx = RenameContext(**json.loads(context))
	try:
		return json.dumps(renamed(name, function, given, ctx)), None
	except Exception as error:
		return None, f"{type(error).__name__}: {error}"


def finish_run() -> None:
	"""Close every client the run built (their `close()`, when they have one)."""
	RENAMES.release()
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
		if folder == root or root not in folder.parents:
			raise DiscoveryError(
				f"python_paths entry {entry!r} must be a folder inside the project folder {root}, not the project folder itself or outside it"
			)
		if not folder.is_dir():
			raise DiscoveryError(f"python_paths entry {entry!r} is not a folder under {root}")
		for path in sorted(folder.rglob("*.py")):
			folders = path.relative_to(folder).parts[:-1]
			if any(_skipped(part) for part in folders) or path.name.startswith("_"):
				continue
			if path not in paths:
				paths.append(path)
	return paths


def _skipped(folder: str) -> bool:
	return folder.startswith(".") or folder.startswith("__") or folder in SKIPPED_FOLDERS


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
	if callable(source.settings.get("rename")):
		settings["rename"], _ = RENAMES.key(source.settings["rename"])
	if source.description is not None:
		settings["description"] = source.description
	if source.client_factory is not None:
		settings["shared_client"] = True
	settings["tables"] = {table.name: _table_settings(table) for table in source.tables}
	return {"name": source.name, "module": relative, "settings": settings, "http_paths": _http_paths(source.tables)}


def _http_paths(tables: list[Table]) -> dict[str, Any]:
	"""The `sl.http` paths each of `tables` writes as literals, for `plan` to check, by table name; tables that build no job left out."""
	found = {table.name: literal_paths(table.function) for table in tables}
	return {name: paths for name, paths in found.items() if paths is not None}


def _table_settings(table: Table) -> dict[str, Any]:
	settings: dict[str, Any] = {key: _setting(value) for key, value in table.settings.items() if value is not None}
	if callable(table.settings.get("rename")):
		settings["rename"], _ = RENAMES.key(table.settings["rename"])
	if "table_name" in settings:
		settings["name"] = settings.pop("table_name")
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
		with _clients_lock:
			if source.name not in _clients:
				assert source.client_factory is not None
				_clients[source.name] = source.client_factory(ctx)
			return _clients[source.name]

	return build
