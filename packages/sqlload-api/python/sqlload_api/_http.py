"""HTTP jobs a table function returns or yields: built in Python, run entirely in Rust.

Calling `paginate` or `fan_out` sends no request; it builds a job definition. A table function
returns or yields it, and SQLLoad runs every request, page and fan-out in Rust, parsing each
response once into the table's rows.
"""

import json
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from sqlload_api._data import Field, Fields, field_list
from sqlload_api._transform import TRANSFORMS

#: What moved from `sl.http` onto the `http` connection in sqlload.toml.
CONNECTION_SETTINGS = ("auth", "rate_limit", "max_in_flight", "retries")

ROW_LOOP_HINT = (
	"rows of an sl.http job stay in Rust and are never handed to Python one at a time (rule python_row_loop); "
	"return or yield the job itself, and filter the rows downstream, or reshape each page with transform="
)


class ConnectionOwner(Protocol):
	"""What `sl.http` reads a connection from: a SQLLoad `sl.source(...)`, or anything with a name and settings."""

	@property
	def name(self) -> str: ...

	@property
	def settings(self) -> Mapping[str, Any]: ...


@dataclass(frozen=True)
class Paginator:
	"""How the next page is found; every paginate names one."""

	spec: Mapping[str, Any]


@dataclass(frozen=True)
class Cells:
	"""How each row's list of cells becomes columns; `paginate` and `fan_out` take it as `cells=`."""

	spec: Mapping[str, Any]


@dataclass(frozen=True)
class CellName:
	"""Where the cells' column names come from: `sl.listed(...)` or `sl.header_row()`; `sl.cells` takes it as `name=`."""

	spec: Mapping[str, Any]


def cells(pointer: str = "$.cells", *, value: str | Sequence[str] = "$.value", name: str | CellName) -> Cells:
	"""Each row's cells at `pointer` as one column per named column, typed like any other value; a missing cell loads NULL.

	`value` is where a cell's value is, or several places tried in turn. `name` is where each cell's column name comes
	from: a path in the cell (`"$.FIELDNAME"`), so each cell names itself; `sl.listed("$.columns")`, the columns the
	response lists; or `sl.header_row()`, the first row's cells. The names then load like any other column's, through
	`naming` and `rename`."""
	values = [value] if isinstance(value, str) else list(value)
	if isinstance(name, str):
		named: dict[str, Any] = {"kind": "field", "path": name}
	elif isinstance(name, CellName):
		named = dict(name.spec)
	else:
		raise TypeError(f'sl.cells name= takes a path in the cell such as "$.name", sl.listed(...) or sl.header_row(), not {name!r}')
	return Cells({"pointer": pointer, "value": values, "name": named})


def listed(path: str, /, *, key: str | None = "columnId", id_key: str = "id", name_key: str | None = "title", by_position: bool = False) -> CellName:
	"""The columns a response lists at `path` (such as `"$.columns"`; a page without the list keeps the names before it).

	Each cell's `key` field is matched to a listed column's `id_key`, and the column is named by its `name_key`
	(Smartsheet: the defaults); `name_key=None` when each listed column is its name. `by_position=True` matches cells to
	the listed columns by position instead. A cell whose column is not listed fails the load."""
	return CellName({"kind": "listed", "from": path, "key": None if by_position else key, "id_key": id_key, "name_key": name_key})


def header_row() -> CellName:
	"""The first row's cells name the columns, matched by position (Google's grid data); that row does not load."""
	return CellName({"kind": "header_row"})


def single() -> Paginator:
	"""One request, no further pages."""
	return Paginator({"kind": "single"})


def next_link(pointer: str, *, read_ahead: int | None = None) -> Paginator:
	"""The next page's URL at `pointer` in the body (such as `"$.next"`); ends when missing, null or empty."""
	_no_read_ahead("sl.next_link", read_ahead)
	return Paginator({"kind": "next_link", "pointer": pointer})


def link_header(*, read_ahead: int | None = None) -> Paginator:
	"""The `Link` header's `rel="next"` URL."""
	_no_read_ahead("sl.link_header", read_ahead)
	return Paginator({"kind": "link_header"})


def offset(
	param: str = "offset", *, limit: int, limit_param: str = "limit", location: str = "query", read_ahead: int | None = None
) -> Paginator:
	"""`param` counts rows from 0 by `limit`; ends at a page with fewer than `limit` rows.

	With `location="body"`, `param` and `limit_param` are paths in the POST's JSON body, such as `"$.page.offset"`.
	With `read_ahead=N`, the pages after a full first page are requested N at a time and still loaded in page order."""
	return Paginator(
		{"kind": "offset", "param": param, "limit_param": limit_param, "limit": limit, "location": _location(location), **_read_ahead(read_ahead)}
	)


def page_number(
	param: str = "page",
	*,
	start: int = 1,
	page_size: int | None = None,
	page_size_param: str | None = "page_size",
	location: str = "query",
	read_ahead: int | None = None,
) -> Paginator:
	"""`param` counts pages from `start`; ends at an empty page, or with `page_size=N` at a page with fewer than N rows.

	`page_size` is sent as `page_size_param` with every page; `page_size_param=None` sends nothing, for an API whose
	page size is fixed. A short page is checked with one more request until the API has shown it doesn't cap its pages
	lower: an API that does still loads every row, paged to an empty page.
	With `location="body"`, `param` and `page_size_param` are paths in the JSON body.
	With `read_ahead=N`, the pages after a full first page are requested N at a time and still loaded in page order."""
	sized: dict[str, Any] = {}
	if page_size is not None:
		if isinstance(page_size, bool) or not isinstance(page_size, int):
			raise TypeError(f"sl.page_number page_size takes a whole number of rows, not {page_size!r}")
		if page_size < 1:
			raise ValueError(f"sl.page_number page_size must be at least 1, not {page_size}")
		sized = {"page_size": page_size, "page_size_param": page_size_param}
	elif page_size_param != "page_size":
		raise ValueError("sl.page_number page_size_param needs page_size: the number of rows a page holds")
	return Paginator(
		{"kind": "page_number", "param": param, "start": start, **sized, "location": _location(location), **_read_ahead(read_ahead)}
	)


def cursor(pointer: str, param: str, *, location: str = "query", read_ahead: int | None = None) -> Paginator:
	"""The token at `pointer` in the response goes back as query parameter `param`; ends when missing, null or empty.

	With `location="body"`, `param` is a path in the POST's JSON body, such as `"$.page.cursor"`."""
	_no_read_ahead("sl.cursor", read_ahead)
	return Paginator({"kind": "cursor", "pointer": pointer, "param": param, "location": _location(location)})


def _read_ahead(read_ahead: int | None) -> dict[str, Any]:
	"""`read_ahead` as a paginator field, left out when unset so paginators without it are unchanged."""
	if read_ahead is not None and (isinstance(read_ahead, bool) or not isinstance(read_ahead, int)):
		raise TypeError(f"read_ahead takes a whole number of pages, not {read_ahead!r}")
	return {} if read_ahead is None else {"read_ahead": read_ahead}


def _no_read_ahead(paginator: str, read_ahead: int | None) -> None:
	if read_ahead is not None:
		raise ValueError(
			f"{paginator} cannot take read_ahead: each page's address comes from the page before it; read_ahead needs sl.offset or sl.page_number"
		)


def _location(location: str) -> str:
	if location not in ("query", "body"):
		raise ValueError(f'location must be "query" or "body", not {location!r}')
	return location


def _param_text(value: Any) -> str:
	"""A request parameter JSON has no form for, such as a `datetime` cursor value: ISO text for dates and times, else `str()`."""
	isoformat = getattr(value, "isoformat", None)
	return isoformat() if callable(isoformat) else str(value)


class Job:
	"""An HTTP job definition; a table function returns or yields it, and Rust runs it."""

	def __init__(self, definition: dict[str, Any], transform: Callable[..., Any] | str | None = None) -> None:
		self._definition = definition
		#: Kept as given, and looked up when the job is handed to Rust: a job built at module level outlives the run that
		#: first registered a function passed as itself, and each run lets go of those when it finishes.
		self._transform = transform

	def __sqlload_http_job__(self) -> str:
		definition = self._definition
		if self._transform is not None:
			function, name = TRANSFORMS.key(self._transform)
			definition = {**definition, "transform": {"function": function, "name": name}}
		return json.dumps(definition, default=_param_text)

	def __iter__(self) -> Iterator["Job"]:
		"""`yield from job` hands the job itself to Rust."""
		yield self

	def __getitem__(self, key: Any) -> Any:
		raise TypeError(ROW_LOOP_HINT)

	def get(self, key: Any, default: Any = None) -> Any:
		raise TypeError(ROW_LOOP_HINT)

	def __repr__(self) -> str:
		return f"sl.http job {json.dumps(self._definition)}"


class Http:
	"""Requests to one HTTP connection, a source's or one named: `paginate` and `fan_out` build jobs."""

	def __init__(
		self,
		source: ConnectionOwner | None = None,
		*,
		headers: Mapping[str, str] | None = None,
		connection: str | None = None,
		**moved: Any,
	) -> None:
		name = connection or (None if source is None else source.settings.get("connection"))
		if not name and source is None:
			raise ValueError("sl.http needs a connection: pass connection=, or a source that sets one")
		if not name:
			raise ValueError(f"sl.http needs a connection: set connection= on source {source.name} or pass connection=")
		for key in moved:
			if key in CONNECTION_SETTINGS:
				raise TypeError(f"sl.http no longer takes {key}: set {key} on [connections.{name}] in sqlload.toml")
			raise TypeError(f"sl.http got an unexpected keyword argument {key!r}")
		self._client = {"connection": name, "headers": dict(headers or {})}

	def paginate(
		self,
		path: str,
		*,
		paginator: Paginator,
		params: Mapping[str, Any] | None = None,
		select: str | None = None,
		on_status: Mapping[int, str] | None = None,
		fields: Fields | None = None,
		keep_record: str | None = None,
		method: str = "GET",
		json_body: Mapping[str, Any] | None = None,
		key: str | None = None,
		cells: Cells | None = None,
		select_missing: str = "fail",
		error_path: str | None = None,
		transform: Callable[..., Any] | str | None = None,
	) -> Job:
		"""Every page of `path`; `select` (such as `"$.items"`) is where the rows are, `on_status={404: "skip"}` treats a status as no rows.

		A response without the `select` path fails the load, naming what it holds, unless `select_missing="empty"` reads it as
		no rows, for APIs that leave an empty list out (as Google's do); an empty list or null is no rows either way.
		`error_path` (such as `"$.errors"`) is where an API answering 200 reports an error: any value there but null,
		false, `""`, `[]` or `{}` fails the table with it, before that page's rows load.

		`fields` loads only those values of each row, renamed (`{"account_code": "$.code"}`); `keep_record` adds each
		row's whole object as a JSON column. `method="POST"` sends `json_body` as JSON with every page, each page's copy
		holding the paginator's values when its `location` is `"body"`. As a fan-out's `over`, `key` names the
		placeholder the selected values fill when it is not the field's own name (`select="$.items[*].id", key="order_id"`).

		`transform` (a function, or the name `@sl.transform` registered one under) is called once per page, in page
		order, on the page's rows after `select`, `fields` and `cells`, as `f(rows, ctx)`; what it returns or yields loads."""
		request = self._request(path, paginator, params, select, on_status, (method, json_body))
		return Job(
			{
				"kind": "paginate",
				**request,
				**_projection(fields, keep_record),
				**_key(key),
				**_cells(cells),
				**_select_missing(select_missing),
				**_error_path(error_path),
			},
			_transform(transform),
		)

	def fan_out(
		self,
		path: str,
		*,
		over: Job | Iterable[Any],
		concurrency: int = 8,
		paginator: Paginator | None = None,
		params: Mapping[str, Any] | None = None,
		select: str | None = None,
		on_status: Mapping[int, str] | None = None,
		fields: Fields | None = None,
		keep_record: str | None = None,
		method: str = "GET",
		json_body: Mapping[str, Any] | None = None,
		key: str | None = None,
		cells: Cells | None = None,
		batch: int | None = None,
		select_missing: str = "fail",
		error_path: str | None = None,
		key_path: str | Field | None = None,
		transform: Callable[..., Any] | str | None = None,
	) -> Job:
		"""Every page of `path` once per key of `over`, up to `concurrency` at a time: `over` is a paginate whose
		`select` picks the keys (`"$.items[*].id"` fills `{id}`), or values (`["uk", "fr"]` fill the one placeholder;
		dicts fill them by name).

		`over` can also be another fan_out whose `select` picks keys, as deep as they nest: each key keeps the
		fields of the keys above it, so a third level's path can use `{order_id}` and `{item_id}`. `key` names the
		placeholder this fan-out's selected values fill when it is itself an `over`.

		With `method="POST"`, `{name}` placeholders in `json_body`'s string values are filled from each key as well:
		a value that is just `"{id}"` takes the key as it is (a number stays a number), and `plan` checks a level
		above gives every one. `batch=1000` sends up to 1000 keys per request instead, each such value holding the
		list of the batch's keys (`{"ids": "{id}"}` sends `{"ids": [1, 2, 3]}`); each batch is one request,
		retried as a whole. `select_missing` and `error_path` are as for `paginate`.

		`key_path="$.issueId"` (from each record, or `sl.parent("$.issueId")` from an object around it) fails the
		table when a record's key is not among the keys its request sent (with `batch=`, the batch's), catching an
		API that answers for other keys.

		`transform` is as for `paginate`, once per page of each key; its `ctx.key` holds the key's fields (`None` with
		`batch=`)."""
		if isinstance(over, (str, bytes)):
			raise TypeError("fan_out over= takes a paginate or fan_out job or a list of values, not one string")
		if isinstance(over, Job) and over._transform is not None:
			raise TypeError("transform is for the job whose rows load, not for the job given as fan_out's over=")
		over_spec = dict(over._definition) if isinstance(over, Job) else {"kind": "values", "values": list(over)}
		request = self._request(path, paginator or single(), params, select, on_status, (method, json_body))
		projected = _projection(fields, keep_record)
		batched = {} if batch is None else {"batch": batch}
		return Job(
			{
				"kind": "fan_out",
				**request,
				**projected,
				**_key(key),
				**_cells(cells),
				**_select_missing(select_missing),
				**_error_path(error_path),
				**_key_path(key_path),
				"over": over_spec,
				"concurrency": concurrency,
				**batched,
			},
			_transform(transform),
		)

	def _request(
		self,
		path: str,
		paginator: Paginator,
		params: Mapping[str, Any] | None,
		select: str | None,
		on_status: Mapping[int, str] | None,
		body: tuple[str, Mapping[str, Any] | None],
	) -> dict[str, Any]:
		method, json_body = body
		if method.upper() not in ("GET", "POST"):
			raise ValueError(f'method must be "GET" or "POST", not {method!r}')
		return {
			"client": self._client,
			"path": path,
			"params": dict(params or {}),
			"method": method.upper(),
			"json_body": None if json_body is None else json.loads(json.dumps(json_body, default=_param_text)),
			"paginator": dict(paginator.spec),
			"select": select,
			"on_status": {str(status): action for status, action in (on_status or {}).items()},
		}


def _key(key: str | None) -> dict[str, Any]:
	"""`key` as a job field, left out when unset so jobs without it are unchanged."""
	return {} if key is None else {"key": key}


def _cells(cells: Cells | None) -> dict[str, Any]:
	"""`cells` as a job field, left out when unset so jobs without it are unchanged."""
	return {} if cells is None else {"cells": dict(cells.spec)}


def _select_missing(select_missing: str) -> dict[str, Any]:
	"""`select_missing` as a job field, left out at its default so jobs without it are unchanged."""
	if select_missing not in ("fail", "empty"):
		raise ValueError(f'select_missing must be "fail" or "empty", not {select_missing!r}')
	return {} if select_missing == "fail" else {"select_missing": select_missing}


def _error_path(error_path: str | None) -> dict[str, Any]:
	"""`error_path` as a job field, left out when unset so jobs without it are unchanged."""
	return {} if error_path is None else {"error_path": error_path}


def _key_path(key_path: str | Field | None) -> dict[str, Any]:
	"""`key_path` as a job field, left out when unset so jobs without it are unchanged."""
	if key_path is None:
		return {}
	if isinstance(key_path, str):
		return {"key_path": {"path": key_path}}
	if isinstance(key_path, Field) and key_path.source is not None and key_path.source.get("kind") == "parent":
		return {"key_path": {"path": key_path.path, "from": dict(key_path.source)}}
	raise TypeError(f'key_path takes a path such as "$.issueId" or sl.parent(...), not {key_path!r}')


def _transform(transform: Callable[..., Any] | str | None) -> Callable[..., Any] | str | None:
	"""`transform` as given, refused here when it is neither a function nor a name."""
	if transform is not None and not isinstance(transform, str) and not callable(transform):
		raise TypeError(f"expected a function or the name @sl.transform registered one under, not {type(transform).__name__}")
	return transform


def _projection(fields: Fields | None, keep_record: str | None) -> dict[str, Any]:
	listed = field_list(fields)
	return {
		"fields": listed,
		"keep_record": keep_record,
	}


def http(
	source: ConnectionOwner | None = None,
	*,
	headers: Mapping[str, str] | None = None,
	connection: str | None = None,
	**moved: Any,
) -> Http:
	"""Requests to `source`'s HTTP connection (or `connection`, which is all it needs without a source), adding `headers`
	to the connection's. The connection sets how requests authenticate (`auth`), the limits every request to it shares
	in the run (`rate_limit`, `max_in_flight`) and its `retries`. The job is the same whichever way the connection is
	given."""
	return Http(source, headers=headers, connection=connection, **moved)
