"""`@sl.transform`: a function that reshapes each file, Google Sheets range or `sl.http` page before it loads.

`sqlload.toml` picks one by the name it is registered under (`transform = "kpi_sheet"`); an `sl.http` job takes the name
or the function itself (`api.paginate(..., transform=kpi_page)`). SQLLoad reads the unit, calls `f(rows, ctx)` with its
rows as a pyarrow Table, and loads what the function returns or yields.
"""

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypeVar, overload

from sqlload_api._registry import Registry

_Function = TypeVar("_Function", bound=Callable[..., Any])

#: The functions `@sl.transform` registered, by name.
TRANSFORMS = Registry("@sl.transform")

#: What a transform may return or yield, for its errors.
RETURNS = "dicts, lists of dicts, a pyarrow Table, or a polars or pandas DataFrame"


@overload
def transform(function: _Function, /) -> _Function: ...


@overload
def transform(name: str | None = None, /) -> Callable[[_Function], _Function]: ...


def transform(function: Any = None, /) -> Any:
	"""Register a transform under its own name (`@sl.transform`) or another (`@sl.transform("kpis")`), for
	`transform = "..."` in `sqlload.toml` or `transform=` on an `sl.http` job to pick. Returns the function unchanged."""
	if function is None or isinstance(function, str):
		name = function

		def register(decorated: _Function) -> _Function:
			TRANSFORMS.register(name or decorated.__name__, decorated)
			return decorated

		return register
	TRANSFORMS.register(function.__name__, function)
	return function


@dataclass(frozen=True)
class TransformContext:
	"""What a transform receives with the rows: the unit they came from.

	`unit` is `"file"`, `"range"` or `"page"`. A file sets `path` (as `_sqlload_source_file` names it,
	`archive.zip/member.csv` for a ZIP member), `modified_at`, and `sheet`, `range` or `skip_rows` when its format uses
	them. A Google Sheets range sets `spreadsheet_id`, `sheet` (the tab) and `range`. An `sl.http` page sets `page`
	(counted from 1 within its request) and, in a fan-out, `key`: the fields of the key it was requested for.
	"""

	unit: str
	table: str
	path: str | None = None
	modified_at: datetime | None = None
	sheet: str | None = None
	range: str | None = None
	skip_rows: int | None = None
	spreadsheet_id: str | None = None
	page: int | None = None
	key: dict[str, Any] | None = None


class Returned:
	"""What a transform returned or yielded, one item at a time, each checked; `dicts` says whether any were dicts."""

	def __init__(self, name: str, items: Iterator[Any]) -> None:
		self._name = name
		self._items = items
		self.dicts = False

	def __iter__(self) -> "Returned":
		return self

	def __next__(self) -> Any:
		item = next(self._items)
		if isinstance(item, dict) or (isinstance(item, list) and all(isinstance(row, dict) for row in item)):
			self.dicts = True
			return item
		if _arrow(item):
			return item
		raise TypeError(f"transform {self._name} yielded {_kind(item)}; yield {RETURNS}")

	def close(self) -> None:
		"""Close a generator the load stopped reading, so its `finally` blocks run now."""
		close = getattr(self._items, "close", None)
		if callable(close):
			close()


def returned_items(name: str, returned: Any) -> tuple[Returned | None, str | None]:
	"""What `returned` crosses as, or why it can't load: a list of dicts, a dict, Arrow data or a DataFrame crosses as one
	item; an iterator (a generator) crosses item by item as it yields."""
	if isinstance(returned, list) and all(isinstance(row, dict) for row in returned):
		return Returned(name, iter([returned] if returned else [])), None
	if isinstance(returned, dict) or _arrow(returned):
		return Returned(name, iter([returned])), None
	if isinstance(returned, Iterator):
		return Returned(name, returned), None
	return None, f"it returned {_kind(returned)}; return or yield {RETURNS}"


def _arrow(value: Any) -> bool:
	return hasattr(value, "__arrow_c_stream__") or hasattr(value, "__arrow_c_array__")


def _kind(value: Any) -> str:
	return "a list holding something other than dicts" if isinstance(value, list) else type(value).__name__
