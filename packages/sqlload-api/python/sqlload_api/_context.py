"""What a table function receives: `Context`, and the connections it reaches through `Connection`.

A host builds a table's context from the engine's read (`Context.from_read`), as SQLLoad does; a host with fields of its
own subclasses `Context`, so the rules that fill it (connections, cursor types, time zones, logged state) are SQLLoad's.
"""

import json
import logging
from collections.abc import Callable, Collection, Iterator, Mapping
from datetime import date, datetime, timezone
from decimal import Decimal
from logging import Logger
from typing import Any
from zoneinfo import ZoneInfo


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
	`lookback`, `cursor_start` on a first run, or `--start-cursor-*`), and `cursor_end` where to stop
	(not included); `None` means open. Times arrive in the table's `timezone`, else UTC. Rows the
	function returns outside the window fail the table, unless `on_cursor_out_of_range` says to skip
	them or load them with a warning.

	With `state="logged"`, `state` is the position the last successful run left (`None` on the
	first run, after `--reload` or `sqlload logged-state reset --clear`); set `ctx.state` to the new
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

	@classmethod
	def from_read(
		cls,
		read: Mapping[str, Any],
		*,
		client_factory: "Callable[[Context], Any] | None" = None,
		logger: Logger | None = None,
		**extras: Any,
	) -> "Context":
		"""The context of the table the engine's read `read` (its `TableRead`, as JSON parsed) names, filled as SQLLoad
		fills it: the source's connection with the others no target writes to, the cursor window in its Python type and the
		table's `timezone`, the logged state, and whether it is a reload. `extras` go to the constructor, for a subclass's
		own fields (`class LoaderContext(Context)`)."""
		connections: Mapping[str, Any] = read.get("connections") or {}
		name = read.get("connection")
		connection = Connection(connections.get(name, {}) if name else {}, connections, read.get("withheld") or ())
		cursor = read.get("cursor") or {}
		zone = cursor.get("timezone")
		return cls(
			table=read["table"],
			source=read["source"],
			connection=connection,
			logger=logger or logging.getLogger(f"sqlload.{read['source']}.{read['table']}"),
			cursor_column=cursor.get("column"),
			cursor_value=cursor_value(cursor.get("start"), cursor.get("column_type"), zone),
			cursor_end=cursor_value(cursor.get("end"), cursor.get("column_type"), zone),
			cursor_inclusive=cursor.get("inclusive", True),
			is_reload=read.get("is_reload", False),
			client_factory=client_factory,
			logged=read.get("logged", False),
			state=json.loads(read["state"]) if read.get("state") is not None else None,
			**extras,
		)

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


def cursor_value(text: str | None, column_type: str | None, zone: str | None = None) -> Any:
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
			return zoned(_guessed_value(text), zone)
		if kind in ("int", "smallint"):
			return int(text)
		if kind == "float":
			return float(text)
		if kind == "decimal":
			return Decimal(text)
		if kind == "date":
			return date.fromisoformat(text[:10])
		if kind in ("timestamp_tz", "timestamp_ntz"):
			return zoned(_timestamp(text, aware=kind == "timestamp_tz"), zone)
	except (ValueError, ArithmeticError):
		return text
	return text


def _guessed_value(text: str) -> Any:
	if text.lstrip("-").isdigit():
		return int(text)
	if len(text) == 10:
		return date.fromisoformat(text)
	value = datetime.fromisoformat(offset_text(text))
	return value.astimezone(timezone.utc) if value.tzinfo is not None else value


def offset_text(text: str) -> str:
	"""`text` with a `Z` or `+00` offset written as `+00:00`, which `fromisoformat` reads on Python 3.10."""
	normal = text.replace("Z", "+00:00")
	return normal + ":00" if normal.endswith("+00") else normal


def zoned(value: Any, zone: str | None) -> Any:
	"""`value` in the IANA zone `zone` when it is a datetime and `zone` is set: an aware one converted, a naive one (a
	wall-clock time declared to be in that zone) given it; anything else as it is."""
	if zone is None or not isinstance(value, datetime):
		return value
	where = ZoneInfo(zone)
	return value.astimezone(where) if value.tzinfo is not None else value.replace(tzinfo=where)


def _timestamp(text: str, *, aware: bool) -> datetime:
	value = datetime.fromisoformat(offset_text(text))
	if not aware:
		return value.replace(tzinfo=None)
	return value.astimezone(timezone.utc) if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
