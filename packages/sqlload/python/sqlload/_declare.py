"""Declaring Python sources and tables.

A source is a module-level object tables attach to; a table is a decorated generator function.
Both are found by scanning the modules under `python_paths`, so declaring one never runs it.
"""

from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from logging import Logger
from typing import Any, TypeVar, overload

TableFunction = Callable[["Context"], Iterator[Any]]
ClientFactory = Callable[["Context"], Any]
_Function = TypeVar("_Function", bound=TableFunction)

#: Settings a table takes, and a source sets as defaults for its tables.
TABLE_SETTINGS: frozenset[str] = frozenset(
	{
		"auto_widen",
		"columns",
		"contract",
		"cursor_column",
		"cursor_start",
		"exclude_columns",
		"flatten",
		"lookback",
		"on_schema_change",
		"schema",
		"tags",
		"type_change_rebuild",
		"unique_key",
		"write_strategy",
	}
)

#: Settings only a source takes.
SOURCE_SETTINGS: frozenset[str] = frozenset({"connection", "destination", "format", "path"})


@dataclass(frozen=True)
class Table:
	"""A table: its generator function and settings; calling it calls the function."""

	name: str
	function: TableFunction
	description: str | None = None
	source: "Source | None" = None
	settings: Mapping[str, Any] = field(default_factory=dict)

	def __call__(self, ctx: "Context") -> Iterator[Any]:
		return self.function(ctx)

	def setting(self, key: str) -> Any:
		"""This table's value for `key`, else its source's default, else None."""
		if key in self.settings:
			return self.settings[key]
		if self.source is not None:
			return self.source.settings.get(key)
		return None


@dataclass
class Source:
	"""Where tables' data comes from: one system or connection, and the defaults its tables share."""

	name: str
	description: str | None = None
	settings: Mapping[str, Any] = field(default_factory=dict)
	tables: list[Table] = field(default_factory=list)
	client_factory: ClientFactory | None = None

	@overload
	def table(self, function: _Function, /) -> Table: ...

	@overload
	def table(
		self, *, name: str | None = None, description: str | None = None, **settings: Any
	) -> Callable[[TableFunction], Table]: ...

	def table(
		self,
		function: TableFunction | None = None,
		/,
		*,
		name: str | None = None,
		description: str | None = None,
		**settings: Any,
	) -> Table | Callable[[TableFunction], Table]:
		"""Attach a table to this source: `@shop.table` or `@shop.table(unique_key="id", ...)`."""
		_check_settings(settings, TABLE_SETTINGS, "table")

		def attach(table_function: TableFunction) -> Table:
			declared = Table(
				name=name or table_function.__name__,
				function=table_function,
				description=description,
				source=self,
				settings=dict(settings),
			)
			self.tables.append(declared)
			return declared

		return attach(function) if function is not None else attach

	def client(self, factory: ClientFactory) -> ClientFactory:
		"""Declare the client this source's tables share as `ctx.client`, built once per run."""
		if self.client_factory is not None:
			raise ValueError(f"source {self.name} already has a client")
		self.client_factory = factory
		return factory


def source(name: str, *, description: str | None = None, **settings: Any) -> Source:
	"""A source tables attach to with `@<source>.table(...)`; `settings` are its connection,
	destination and the defaults its tables share."""
	_check_settings(settings, TABLE_SETTINGS | SOURCE_SETTINGS, "source")
	return Source(name=name, description=description, settings=dict(settings))


@overload
def table(function: _Function, /) -> Table: ...


@overload
def table(
	*, name: str | None = None, description: str | None = None, **settings: Any
) -> Callable[[TableFunction], Table]: ...


def table(
	function: TableFunction | None = None,
	/,
	*,
	name: str | None = None,
	description: str | None = None,
	**settings: Any,
) -> Table | Callable[[TableFunction], Table]:
	"""A standalone table, with the target's defaults: `@sl.table` or `@sl.table(...)`."""
	_check_settings(settings, TABLE_SETTINGS, "table")

	def declare(table_function: TableFunction) -> Table:
		return Table(
			name=name or table_function.__name__,
			function=table_function,
			description=description,
			settings=dict(settings),
		)

	return declare(function) if function is not None else declare


class Connection(Mapping[str, Any]):
	"""A connection's resolved settings; calling it looks up another connection by name."""

	def __init__(self, settings: Mapping[str, Any], named: Mapping[str, Mapping[str, Any]]) -> None:
		self._settings = dict(settings)
		self._named = named

	def __getitem__(self, key: str) -> Any:
		return self._settings[key]

	def __iter__(self) -> Iterator[str]:
		return iter(self._settings)

	def __len__(self) -> int:
		return len(self._settings)

	def __call__(self, name: str) -> "Connection":
		if name not in self._named:
			raise KeyError(f"no connection named {name}")
		return Connection(self._named[name], self._named)


@dataclass
class Context:
	"""What a table function receives: its connection and client, the cursor, and a logger."""

	connection: Connection
	logger: Logger
	client: Any = None
	cursor_value: Any = None
	is_reload: bool = False


def _check_settings(settings: Mapping[str, Any], allowed: frozenset[str], what: str) -> None:
	unknown = sorted(set(settings) - allowed)
	if unknown:
		raise TypeError(f"unknown {what} setting(s): {', '.join(unknown)}")
