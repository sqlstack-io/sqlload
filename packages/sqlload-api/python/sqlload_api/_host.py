"""What every host's crossing calls in Python: transforms (one registry per process, whichever host runs) and the crossing's
own callbacks; and the logged-state bookkeeping a host's runtime answers the engine with.

SQLLoad's engine (`sqlload-python-host`) imports this module by name. A host's own runtime module defines discovery and how
it calls its table functions; nothing here depends on the host.
"""

import hashlib
import importlib
import json
import types
from collections.abc import Callable, Iterator
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlload_api._context import Context, offset_text
from sqlload_api._data import checkpoint
from sqlload_api._transform import TRANSFORMS, TransformContext, returned_items

#: What `page_items` yields after each page's items.
PAGE_END = checkpoint(None)

#: How long a transform's version hash is, in hex digits.
VERSION_DIGITS = 16

#: The project folder the host imported from, which errors name files relative to.
_root: list[Path] = []


def set_root(root: Path) -> None:
	"""Name files relative to `root` (the project folder) in transforms' errors."""
	_root[:] = [root]


def crossing_default(value: Any) -> Any:
	"""orjson's `default` for yielded rows: `str()`, a `time` as ISO text; a `Decimal`, `date`, `datetime` or bytes refuses, so the batch is walked and keeps its type."""
	if isinstance(value, (Decimal, bytes, bytearray, memoryview)) or type(value) in (date, datetime):
		raise TypeError("typed value")
	if type(value) is time:
		return value.isoformat()
	return str(value)


def transform_version(name: str) -> tuple[str | None, str | None]:
	"""Look up the transform registered as `name` and check it can run.

	Returns its version, `name@` and a hash of the file it is defined in (an edit to the file, not only to the function,
	changes it; when the file can't be read, as for a function defined in a notebook, a hash of the function's code), or
	why it cannot run: no function or two are registered as `name`, or pyarrow is missing.
	"""
	function, problem = TRANSFORMS.lookup(name, _root[0] if _root else None)
	if function is None:
		return None, problem
	try:
		importlib.import_module("pyarrow")
	except ImportError:
		return None, "transform needs pyarrow: pip install pyarrow"
	source = TRANSFORMS.source_file(name) or ""
	contents = Path(source).read_bytes() if source and Path(source).is_file() else _code_bytes(function)
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
	ctx = TransformContext(**{**fields, "modified_at": datetime.fromisoformat(offset_text(modified_at)) if modified_at else None})
	return returned_items(TRANSFORMS.shown(name), function(table, ctx))


def page_items(next_page: Callable[[], Any]) -> Iterator[Any]:
	"""Yield what an `sl.http` job's transform returns for each page `next_page` hands over, item by item, and
	`sl.checkpoint(None)` after each page's items to mark where they end, until it hands over `None`."""
	while (returned := next_page()) is not None:
		yield from returned
		yield PAGE_END


def finish() -> None:
	"""The run is over: let go of the functions passed to jobs as themselves (`transform=page`)."""
	TRANSFORMS.release()


def current_state(ctx: Context | None) -> str | None:
	"""The JSON `ctx.state` was assigned one assignment before its latest, while the table function runs: rows crossing now commit with it.

	Rows a function yields may cross before or after it moves `ctx.state` past them, so only the
	previous value is sure to cover every row crossed so far, whether the state is set before or
	after the rows it covers; a resumed run then never skips rows, and may load up to about two
	pages again. Each assignment is kept as JSON, so re-assigning a mutated object leaves the
	previous value as it was."""
	if ctx is None or not ctx._logged:
		return None
	return ctx._previous_assigned_state


def final_state(ctx: Context | None) -> str | None:
	"""The position the table function left in `ctx.state`, as JSON, when it differs from the one it started with.

	It is read once the function finished, so a position changed in place (`ctx.state["token"] = ...`)
	counts like one assigned.
	"""
	if ctx is None or not ctx._logged:
		return None
	try:
		final = json.dumps(ctx.state)
	except (TypeError, ValueError) as error:
		raise TypeError(f"ctx.state of table {ctx.table} is not JSON: {error}") from error
	return final if final != ctx.started_state else None


def _code_bytes(function: Any) -> bytes:
	"""What a function does, for its version when its file can't be read: its bytecode, constants (nested functions' code
	included) and the names it uses, without its file name or line numbers. A callable without code, by its type's name."""
	code = getattr(function, "__code__", None)
	return _code_parts(code) if isinstance(code, types.CodeType) else type(function).__qualname__.encode()


def _code_parts(code: types.CodeType) -> bytes:
	constants = [_code_parts(constant) if isinstance(constant, types.CodeType) else _constant_text(constant) for constant in code.co_consts]
	return b"\0".join([code.co_code, repr(code.co_names).encode(), *constants])


def _constant_text(constant: Any) -> bytes:
	"""A code constant as text that is the same in every process: a frozenset's items sorted, as its order follows string hashing."""
	if isinstance(constant, frozenset):
		return repr(sorted(repr(item) for item in constant)).encode()
	if isinstance(constant, tuple):
		return b"(" + b",".join(_constant_text(item) for item in constant) + b")"
	return repr(constant).encode()
