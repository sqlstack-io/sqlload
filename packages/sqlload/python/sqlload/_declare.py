"""Declaring Python sources and tables.

A source is a module-level object tables attach to; a table is a decorated generator function.
Both are found by scanning the modules under `python_paths`, so declaring one never runs it.
"""

import json
from collections.abc import Callable, Collection, Iterator, Mapping
from dataclasses import dataclass, field
from logging import Logger
from typing import Any, TypeVar, overload

TableFunction = Callable[["Context"], Iterator[Any]]
ClientFactory = Callable[["Context"], Any]
_Function = TypeVar("_Function", bound=TableFunction)

#: Settings a table takes, and a source sets as defaults for its tables (the keys of `sqlload.toml`).
TABLE_SETTINGS: frozenset[str] = frozenset(
	{
		"allow_empty",
		"append_cursor_inclusive",
		"auto_widen",
		"batch",
		"contract",
		"cursor_column",
		"cursor_start",
		"lookback",
		"on_schema_change",
		"state",
		"type_change_rebuild",
		"unique_key",
		"write_strategy",
	}
)

#: Settings only a table takes.
TABLE_ONLY_SETTINGS: frozenset[str] = frozenset(
	{"allow_columns", "columns", "destination_path", "exclude_columns", "mask", "table_name"}
)

#: Settings only a source takes.
SOURCE_SETTINGS: frozenset[str] = frozenset(
	{
		"allow_columns",
		"connection",
		"destination",
		"destination_path",
		"exclude_columns",
		"mask_key",
		"parallel_tables",
		"schema",
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
		"""Attach a table to this source: `@shop.table` or `@shop.table(unique_key="id", ...)`."""
		_check_settings(settings, TABLE_SETTINGS | TABLE_ONLY_SETTINGS, "table")

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
	"""A standalone table, with the target's defaults: `@sl.table` or `@sl.table(...)`."""
	_check_settings(settings, TABLE_SETTINGS | TABLE_ONLY_SETTINGS, "table")

	def declare(table_function: TableFunction) -> Table:
		return Table(
			name=name or table_function.__name__,
			function=table_function,
			description=description,
			settings=dict(settings),
		)

	return declare(function) if function is not None else declare


class Connection(Mapping[str, Any]):
	"""A connection's resolved settings; calling it looks up another connection by name.

	Connections that `[targets]` write to are not available to table functions (except a source's
	own connection), so a table function cannot reach a destination's credentials.
	"""

	def __init__(
		self,
		settings: Mapping[str, Any],
		named: Mapping[str, Mapping[str, Any]],
		withheld: Collection[str] = (),
	) -> None:
		self._settings = dict(settings)
		self._named = named
		self._withheld = frozenset(withheld)

	def __getitem__(self, key: str) -> Any:
		return self._settings[key]

	def __iter__(self) -> Iterator[str]:
		return iter(self._settings)

	def __len__(self) -> int:
		return len(self._settings)

	def __call__(self, name: str) -> "Connection":
		if name in self._withheld:
			raise PermissionError(
				f"connection {name} is used by a [targets] entry; table functions only get connections"
				" that no target writes to, so they cannot reach a destination's credentials"
			)
		if name not in self._named:
			raise KeyError(f"no connection named {name}")
		return Connection(self._named[name], self._named, self._withheld)


class Context:
	"""What a table function receives: its connection and client, the cursor window, and a logger.

	`cursor_value` is where to read from (the destination's maximum of `cursor_column` minus
	`lookback`, `cursor_start` on a first run, or `--start-cursor-*`); `None` means read everything.
	Rows outside the window are skipped at load, so a function that ignores it still loads
	correctly, only slower.

	With `state="logged"`, `state` is the position the last successful run left (`None` on the
	first run, after `--reload` or `sqlload state reset --clear`); set `ctx.state` to the new
	position (any JSON value, such as an API sync token) and SQLLoad records it with the data.
	"""

	def __init__(
		self,
		*,
		table: str,
		source: str,
		connection: Connection,
		logger: Logger,
		cursor_column: str | None = None,
		cursor_value: Any = None,
		cursor_end: Any = None,
		cursor_inclusive: bool = True,
		is_reload: bool = False,
		client_factory: Callable[["Context"], Any] | None = None,
		logged: bool = False,
		state: Any = None,
	) -> None:
		self.table = table
		self.source = source
		self._connection = connection
		self.logger = logger
		self.cursor_column = cursor_column
		self.cursor_value = cursor_value
		self.cursor_end = cursor_end
		self.cursor_inclusive = cursor_inclusive
		self.is_reload = is_reload
		self._client_factory = client_factory
		self._client: Any = None
		self._logged = logged
		self._state = state
		self.started_state = json.dumps(state)
		self._assigned_state = self.started_state
		self._previous_assigned_state = self.started_state

	@property
	def state(self) -> Any:
		"""The logged position (`state="logged"` tables): where the last successful run left off."""
		return self._state

	@state.setter
	def state(self, value: Any) -> None:
		if not self._logged:
			raise AttributeError(f"ctx.state needs state='logged' on table {self.table}")
		try:
			assigned = json.dumps(value)
		except (TypeError, ValueError) as error:
			raise TypeError(f"ctx.state of table {self.table} is not JSON: {error}") from error
		self._previous_assigned_state = self._assigned_state
		self._assigned_state = assigned
		self._state = value

	@property
	def connection(self) -> Connection:
		"""The source's connection settings (empty when it names none); call it with a name for another connection."""
		return self._connection

	@property
	def client(self) -> Any:
		"""The source's client (`@source.client`), built on first use and shared by its tables for the run."""
		if self._client is None:
			if self._client_factory is None:
				raise LookupError(f"the source of table {self.table} declares no @source.client")
			self._client = self._client_factory(self)
		return self._client


def _check_settings(settings: Mapping[str, Any], allowed: frozenset[str], what: str) -> None:
	unknown = sorted(set(settings) - allowed)
	if unknown:
		raise TypeError(f"unknown {what} setting(s): {', '.join(unknown)}")
