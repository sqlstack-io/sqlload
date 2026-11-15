"""Discovering Python sources under `python_paths` and calling their table functions for the loader.

The native loader calls these functions through `sqlload._native` while running `sqlload plan`,
`load` and the other commands; they are not a public API.
"""

import hashlib
import importlib
import json
import logging
import sys
import threading
import traceback
from collections.abc import Iterator
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from sqlload._declare import Connection, Context, Source, Table
from sqlload._http import Job
from sqlload._http_paths import literal_paths
from sqlload._rename import RENAMES, Column, RenameContext, renamed
from sqlload._transform import TRANSFORMS, TransformContext, returned_items

#: Folders under `python_paths` never imported: virtual environments, build output and caches.
SKIPPED_FOLDERS: frozenset[str] = frozenset({"venv", "build", "dist", "site-packages", "node_modules"})

#: The destination name of a table declared without a source: its own name.
STANDALONE_TABLE_NAME = "{table}"

#: How long a transform's version hash is, in hex digits.
VERSION_DIGITS = 16

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


def crossing_default(value: Any) -> Any:
	"""orjson's `default` for yielded rows: `str()`, a `time` as ISO text; a `Decimal`, `date` or `datetime` refuses, so the batch is walked and keeps its type."""
	if isinstance(value, Decimal) or type(value) in (date, datetime):
		raise TypeError("typed value")
	if type(value) is time:
		return value.isoformat()
	return str(value)


def start_table(request: str) -> Iterator[Any]:
	"""Call a table function with its context; returns what it yields."""
	read = json.loads(request)
	table = _tables[read["table"]]
	connections: dict[str, Any] = read["connections"]
	connection_name = read.get("connection")
	withheld: list[str] = read["withheld"]
	connection = Connection(connections.get(connection_name, {}) if connection_name else {}, connections, withheld)
	cursor = read.get("cursor") or {}
	source = table.source
	ctx = Context(
		table=table.name,
		source=read["source"],
		connection=connection,
		logger=logging.getLogger(f"sqlload.{read['source']}.{table.name}"),
		cursor_column=cursor.get("column"),
		cursor_value=_cursor_value(cursor.get("start"), cursor.get("column_type"), cursor.get("timezone")),
		cursor_end=_cursor_value(cursor.get("end"), cursor.get("column_type"), cursor.get("timezone")),
		cursor_inclusive=cursor.get("inclusive", True),
		is_reload=read["is_reload"],
		client_factory=_shared_client(source) if source is not None and source.client_factory else None,
		logged=read.get("logged", False),
		state=json.loads(read["state"]) if read.get("state") is not None else None,
	)
	_contexts[table.name] = ctx
	yielded = table.function(ctx)
	if yielded is None:
		return iter(())
	return iter([yielded]) if isinstance(yielded, Job) else iter(yielded)


def current_state(table: str) -> str | None:
	"""The JSON `ctx.state` was assigned one assignment before its latest, while the table function runs: rows crossing now commit with it.

	Rows a function yields may cross before or after it moves `ctx.state` past them, so only the
	previous value is sure to cover every row crossed so far, whether the state is set before or
	after the rows it covers; a resumed run then never skips rows, and may load up to about two
	pages again. Each assignment is kept as JSON, so re-assigning a mutated object leaves the
	previous value as it was."""
	ctx = _contexts.get(table)
	if ctx is None or not ctx._logged:
		return None
	return ctx._previous_assigned_state


def table_state(table: str) -> str | None:
	"""The position the table function left in `ctx.state`, as JSON, when it differs from the one it started with.

	It is read once the function finished, so a position changed in place (`ctx.state["token"] = ...`)
	counts like one assigned.
	"""
	ctx = _contexts.pop(table, None)
	if ctx is None or not ctx._logged:
		return None
	try:
		final = json.dumps(ctx.state)
	except (TypeError, ValueError) as error:
		raise TypeError(f"ctx.state of table {table} is not JSON: {error}") from error
	return final if final != ctx.started_state else None


def transform_version(name: str) -> tuple[str | None, str | None]:
	"""Look up the transform registered as `name` and check it can run.

	Returns its version, `name@` and a hash of the file it is defined in (an edit to the file, not only to the function,
	changes it), or why it cannot run: no function or two are registered as `name`, or pyarrow is missing.
	"""
	function, problem = TRANSFORMS.lookup(name, _root[0] if _root else None)
	if function is None:
		return None, problem
	try:
		importlib.import_module("pyarrow")
	except ImportError:
		return None, "transform needs pyarrow: pip install pyarrow"
	source = TRANSFORMS.source_file(name) or ""
	contents = Path(source).read_bytes() if source and Path(source).is_file() else name.encode()
	return f"{name}@{hashlib.sha256(contents).hexdigest()[:VERSION_DIGITS]}", None


def call_transform(name: str, table: Any, context: str) -> tuple[Any, str | None]:
	"""Call the transform `name` on one unit's rows, a pyarrow Table, with its `ctx` (JSON).

	Returns what crosses, item by item as a generator yields them (its `dicts` says whether any were dicts), or why the
	returned value cannot load.
	"""
	function, problem = TRANSFORMS.lookup(name)
	if function is None:
		raise LookupError(problem)
	fields: dict[str, Any] = json.loads(context)
	modified_at = fields.get("modified_at")
	ctx = TransformContext(**{**fields, "modified_at": datetime.fromisoformat(_offset_text(modified_at)) if modified_at else None})
	return returned_items(TRANSFORMS.shown(name), function(table, ctx))


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


def _cursor_value(text: str | None, column_type: str | None, zone: str | None = None) -> Any:
	"""A cursor bound in its Python type: int, float, Decimal, date, or datetime: aware for `timestamp_tz`, in the
	table's `timezone` (`zone`) or UTC; for `timestamp_ntz`, aware in `zone` when it is set, its wall-clock time kept.

	Before the first load shows the cursor's type (`unknown`), ISO dates and timestamps and integers
	are read as such, so the value's type matches later runs.
	"""
	if text is None:
		return None
	kind = column_type or "unknown"
	if kind.startswith("decimal"):
		kind = "decimal"
	try:
		if kind == "unknown":
			return _zoned(_guessed_value(text), zone)
		if kind in ("int", "smallint"):
			return int(text)
		if kind == "float":
			return float(text)
		if kind == "decimal":
			return Decimal(text)
		if kind == "date":
			return date.fromisoformat(text[:10])
		if kind in ("timestamp_tz", "timestamp_ntz"):
			return _zoned(_timestamp(text, aware=kind == "timestamp_tz"), zone)
	except (ValueError, ArithmeticError):
		return text
	return text


def _guessed_value(text: str) -> Any:
	if text.lstrip("-").isdigit():
		return int(text)
	if len(text) == 10:
		return date.fromisoformat(text)
	value = datetime.fromisoformat(_offset_text(text))
	return value.astimezone(timezone.utc) if value.tzinfo is not None else value


def _offset_text(text: str) -> str:
	"""`text` with a `Z` or `+00` offset written as `+00:00`, which `fromisoformat` reads on Python 3.10."""
	normal = text.replace("Z", "+00:00")
	return normal + ":00" if normal.endswith("+00") else normal


def _zoned(value: Any, zone: str | None) -> Any:
	"""`value` in the IANA zone `zone` when it is a datetime and `zone` is set: an aware one converted, a naive one (a
	wall-clock time declared to be in that zone) given it; anything else as it is."""
	if zone is None or not isinstance(value, datetime):
		return value
	where = ZoneInfo(zone)
	return value.astimezone(where) if value.tzinfo is not None else value.replace(tzinfo=where)


def _timestamp(text: str, *, aware: bool) -> datetime:
	value = datetime.fromisoformat(_offset_text(text))
	if not aware:
		return value.replace(tzinfo=None)
	return value.astimezone(timezone.utc) if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
