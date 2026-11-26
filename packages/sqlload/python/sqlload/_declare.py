"""Declaring Python sources and tables.

A source is a module-level object tables attach to; a table is a decorated generator function.
Both are found by scanning the modules under `python_paths`, so declaring one never runs it.
"""

import inspect
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, TypeVar, overload

# What a table function receives lives in sqlload_api, which other hosts share; re-exported here unchanged.
from sqlload_api._context import Connection, Context

__all__ = ["Connection", "Context", "Source", "Table", "source", "table"]

TableFunction = Callable[["Context"], Iterator[Any]]
ClientFactory = Callable[["Context"], Any]
_Function = TypeVar("_Function", bound=TableFunction)

#: Settings a table takes, and a source sets as defaults for its tables (the keys of `sqlload.toml`).
TABLE_SETTINGS: frozenset[str] = frozenset(
	{
		"allow_empty",
		"append_cursor_inclusive",
		"auto_widen",
		"batch_concurrency",
		"batch_size",
		"checks",
		"constant_columns",
		"contract",
		"cursor_column",
		"cursor_start",
		"infer_types",
		"lookback",
		"compat",
		"naming",
		"newest_by",
		"null_keys",
		"null_cursor",
		"on_cursor_out_of_range",
		"on_duplicate_keys",
		"on_duplicate_names",
		"on_nonfinite",
		"on_schema_change",
		"rename",
		"replace_key",
		"row_id",
		"send",
		"state",
		"timezone",
		"type_change_rebuild",
		"unique_key",
		"write_strategy",
	}
)

#: Settings only a table takes.
TABLE_ONLY_SETTINGS: frozenset[str] = frozenset(
	{
		"allow_columns",
		"columns",
		"destination_path",
		"exclude_columns",
		"flatten",
		"mask",
		"max_depth",
		"parse",
		"table_name",
		"unnest",
	}
)

#: Settings only a source takes.
SOURCE_SETTINGS: frozenset[str] = frozenset(
	{
		"allow_columns",
		"connection",
		"destination",
		"destination_path",
		"destination_schema",
		"exclude_columns",
		"mask_key",
		"parallel_tables",
		"table_name",
	}
)


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
		"""Attach a table to this source: `@shop.table` or `@shop.table(unique_key="id", ...)`; its
		description, the destination table's comment, is `description` or else the function's docstring."""
		_check_settings(settings, TABLE_SETTINGS | TABLE_ONLY_SETTINGS, "table")

		def attach(table_function: TableFunction) -> Table:
			declared = Table(
				name=name or table_function.__name__,
				function=table_function,
				description=description or _docstring(table_function),
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


def _docstring(function: TableFunction) -> str | None:
	"""A plain function's own docstring, the table's description when none is given."""
	return inspect.getdoc(function) if inspect.isfunction(function) else None


def source(name: str, *, description: str | None = None, **settings: Any) -> Source:
	"""A source tables attach to with `@<source>.table(...)`; `settings` are its connection,
	destination and the defaults its tables share."""
	if "schema" in settings:
		raise TypeError("sl.source() got schema=; use destination_schema=")
	_check_settings(settings, TABLE_SETTINGS | SOURCE_SETTINGS, "source")
	if not isinstance(settings.get("parallel_tables", False), bool):
		raise TypeError(f"source {name}: parallel_tables must be True or False")
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
	"""A standalone table, which sets its own write_strategy: `@sl.table` or `@sl.table(...)`; its
	description, the destination table's comment, is `description` or else the function's docstring."""
	_check_settings(settings, TABLE_SETTINGS | TABLE_ONLY_SETTINGS, "table")

	def declare(table_function: TableFunction) -> Table:
		return Table(
			name=name or table_function.__name__,
			function=table_function,
			description=description or _docstring(table_function),
			settings=dict(settings),
		)

	return declare(function) if function is not None else declare


def _check_settings(settings: Mapping[str, Any], allowed: frozenset[str], what: str) -> None:
	unknown = sorted(set(settings) - allowed)
	if unknown:
		raise TypeError(f"unknown {what} setting(s): {', '.join(unknown)}")
