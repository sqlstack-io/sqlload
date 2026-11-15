"""`@sl.rename`: a function that names a table's columns where `naming` leaves them unfit.

`sqlload.toml` picks one by the name it is registered under (`rename = "erp_names"`); a Python table takes the name or
the function itself (`@src.table(rename=erp_names)`). SQLLoad calls `f(columns, ctx)` once per table with every column
in order, and loads each column under the name the function returns for its position, or its default name.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, TypeVar, overload

from sqlload._registry import Registry

_Function = TypeVar("_Function", bound=Callable[..., Any])

#: The functions `@sl.rename` registered, by name.
RENAMES = Registry("@sl.rename")


@overload
def rename(function: _Function, /) -> _Function: ...


@overload
def rename(name: str | None = None, /) -> Callable[[_Function], _Function]: ...


def rename(function: Any = None, /) -> Any:
	"""Register a rename function under its own name (`@sl.rename`) or another (`@sl.rename("erp")`), for
	`rename = "..."` in `sqlload.toml` or `rename=` on a Python table to pick. Returns the function unchanged."""
	if function is None or isinstance(function, str):
		name = function

		def register(decorated: _Function) -> _Function:
			RENAMES.register(name or decorated.__name__, decorated)
			return decorated

		return register
	RENAMES.register(function.__name__, function)
	return function


@dataclass(frozen=True)
class Column:
	"""A column a rename function is given: its place in the table (from 1), its name in the source, and the name it
	loads as without the rename (after `naming`). Two repeated headers have the same `source` and differ by `position`."""

	position: int
	source: str
	default: str


@dataclass(frozen=True)
class RenameContext:
	"""What a rename function receives with the columns: the source and the table they belong to."""

	source: str
	table: str


def renamed(name: str, function: Callable[..., Any], columns: list[Column], ctx: RenameContext) -> dict[str, str]:
	"""The names `function` gives `columns`, by position (as text, for JSON); an error naming the function when it returns
	anything but a mapping of the columns' positions to names that are not empty."""
	returned = function(columns, ctx)
	if returned is None:
		return {}
	if not isinstance(returned, Mapping):
		raise TypeError(f"it returned {type(returned).__name__}; return a dict of column positions to names")
	positions = {column.position for column in columns}
	names: dict[str, str] = {}
	for position, new in returned.items():
		if not isinstance(position, int) or isinstance(position, bool) or position not in positions:
			raise ValueError(f"it returned the key {position!r}; keys are the columns' positions, 1 to {len(columns)}")
		if not isinstance(new, str) or not new.strip():
			raise ValueError(f"it returned {new!r} for column {position}; give each a name that is not empty")
		names[str(position)] = new
	return names
