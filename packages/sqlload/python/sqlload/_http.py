"""HTTP jobs a table function returns or yields: built in Python, run entirely in Rust.

Calling `paginate` or `fan_out` sends no request; it builds a job definition. A table function
returns or yields it, and SQLLoad runs every request, page and fan-out in Rust, parsing each
response once into the table's rows.
"""

import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from sqlload._declare import Source

#: Default retries of a request that failed with a transport error, 429 or a 5xx.
DEFAULT_RETRIES = 5

ROW_LOOP_HINT = (
	"rows of an sl.http job stay in Rust and are never handed to Python one at a time (rule python_row_loop); "
	"return or yield the job itself, and filter the rows downstream"
)


@dataclass(frozen=True)
class Paginator:
	"""How the next page is found; every paginate names one."""

	spec: Mapping[str, Any]


@dataclass(frozen=True)
class Auth:
	"""Which of the connection's settings authenticate requests."""

	spec: Mapping[str, Any]


def single() -> Paginator:
	"""One request, no further pages."""
	return Paginator({"kind": "single"})


def next_link(pointer: str) -> Paginator:
	"""The next page's URL at `pointer` in the body (such as `"$.next"`); ends when missing, null or empty."""
	return Paginator({"kind": "next_link", "pointer": pointer})


def link_header() -> Paginator:
	"""The `Link` header's `rel="next"` URL."""
	return Paginator({"kind": "link_header"})


def offset(param: str = "offset", *, limit: int, limit_param: str = "limit") -> Paginator:
	"""`param` counts rows from 0 by `limit`; ends at a page with fewer than `limit` rows."""
	return Paginator({"kind": "offset", "param": param, "limit_param": limit_param, "limit": limit})


def page_number(param: str = "page", *, start: int = 1) -> Paginator:
	"""`param` counts pages from `start`; ends at an empty page."""
	return Paginator({"kind": "page_number", "param": param, "start": start})


def cursor(pointer: str, param: str) -> Paginator:
	"""The token at `pointer` in the body goes back as query parameter `param`; ends when missing, null or empty."""
	return Paginator({"kind": "cursor", "pointer": pointer, "param": param})


def bearer() -> Auth:
	"""`Authorization: Bearer <token>`, the connection's `token`."""
	return Auth({"kind": "bearer"})


def api_key(*, header: str | None = None, query: str | None = None) -> Auth:
	"""The connection's `token` in the header or the query parameter named."""
	if (header is None) == (query is None):
		raise ValueError("sl.api_key takes exactly one of header= or query=")
	return Auth({"kind": "api_key", "header": header, "query": query})


def basic() -> Auth:
	"""HTTP basic auth with the connection's `username` and `password`."""
	return Auth({"kind": "basic"})


def oauth2_client_credentials(token_url: str, *, scope: str | None = None, client_auth: str = "basic") -> Auth:
	"""`Authorization: Bearer <access token>`, the token `token_url` issues for the connection's `client_id` and `client_secret`.

	The token is fetched once per run, renewed before it expires and once after a 401. `client_auth` is `"basic"`
	(HTTP basic auth at the token URL) or `"post"` (`client_id` and `client_secret` as form fields).
	"""
	return Auth(_oauth2("oauth2_client_credentials", token_url, scope, client_auth))


def oauth2_refresh_token(token_url: str, *, scope: str | None = None, client_auth: str = "basic") -> Auth:
	"""`Authorization: Bearer <access token>`, the token `token_url` issues for the connection's `refresh_token`.

	The connection's `client_id` and `client_secret` go along when it has them. A refresh token the server rotates is
	used for the rest of the run but not saved, so the next run starts from the connection's `refresh_token` again.
	"""
	return Auth(_oauth2("oauth2_refresh_token", token_url, scope, client_auth))


def _oauth2(kind: str, token_url: str, scope: str | None, client_auth: str) -> dict[str, Any]:
	if client_auth not in ("basic", "post"):
		raise ValueError(f'client_auth must be "basic" or "post", not {client_auth!r}')
	return {"kind": kind, "token_url": token_url, "scope": scope, "client_auth": client_auth}


def _param_text(value: Any) -> str:
	"""A request parameter JSON has no form for, such as a `datetime` cursor value: ISO text for dates and times, else `str()`."""
	isoformat = getattr(value, "isoformat", None)
	return isoformat() if callable(isoformat) else str(value)


class Job:
	"""An HTTP job definition; a table function returns or yields it, and Rust runs it."""

	def __init__(self, definition: dict[str, Any]) -> None:
		self._definition = definition

	def __sqlload_http_job__(self) -> str:
		return json.dumps(self._definition, default=_param_text)

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
	"""Requests to one source's HTTP connection: `paginate` and `fan_out` build jobs."""

	def __init__(
		self,
		source: Source,
		*,
		auth: Auth | None = None,
		rate_limit: str | None = None,
		retries: int = DEFAULT_RETRIES,
		headers: Mapping[str, str] | None = None,
		connection: str | None = None,
	) -> None:
		name = connection or source.settings.get("connection")
		if not name:
			raise ValueError(f"sl.http needs a connection: set connection= on source {source.name} or pass connection=")
		self._client = {
			"connection": name,
			"auth": dict(auth.spec) if auth else {"kind": "none"},
			"rate_limit": rate_limit,
			"retries": retries,
			"headers": dict(headers or {}),
		}

	def paginate(
		self,
		path: str,
		*,
		paginator: Paginator,
		params: Mapping[str, Any] | None = None,
		select: str | None = None,
		on_status: Mapping[int, str] | None = None,
	) -> Job:
		"""Every page of `path`; `select` (such as `"$.items"`) is where the rows are, `on_status={404: "skip"}` treats a status as no rows."""
		return Job({"kind": "paginate", **self._request(path, paginator, params, select, on_status)})

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
	) -> Job:
		"""Every page of `path` once per key of `over`, up to `concurrency` at a time: `over` is a paginate whose
		`select` picks the keys (`"$.items[*].id"` fills `{id}`), or values (`["uk", "fr"]` fill the one placeholder;
		dicts fill them by name)."""
		if isinstance(over, Job) and over._definition["kind"] != "paginate":
			raise TypeError("fan_out over= takes a paginate job or values, not another fan_out")
		if isinstance(over, (str, bytes)):
			raise TypeError("fan_out over= takes a paginate job or a list of values, not one string")
		over_spec = (
			{"kind": "paginate", **{k: v for k, v in over._definition.items() if k != "kind"}}
			if isinstance(over, Job)
			else {"kind": "values", "values": list(over)}
		)
		request = self._request(path, paginator or single(), params, select, on_status)
		return Job({"kind": "fan_out", **request, "over": over_spec, "concurrency": concurrency})

	def _request(
		self,
		path: str,
		paginator: Paginator,
		params: Mapping[str, Any] | None,
		select: str | None,
		on_status: Mapping[int, str] | None,
	) -> dict[str, Any]:
		return {
			"client": self._client,
			"path": path,
			"params": dict(params or {}),
			"paginator": dict(paginator.spec),
			"select": select,
			"on_status": {str(status): action for status, action in (on_status or {}).items()},
		}


def http(
	source: Source,
	*,
	auth: Auth | None = None,
	rate_limit: str | None = None,
	retries: int = DEFAULT_RETRIES,
	headers: Mapping[str, str] | None = None,
	connection: str | None = None,
) -> Http:
	"""Requests to `source`'s HTTP connection (or `connection`), with `auth`, a `rate_limit` such as `"10/s"` shared
	by every job on the connection, and `retries` of transport errors, 429s and 5xx responses."""
	return Http(source, auth=auth, rate_limit=rate_limit, retries=retries, headers=headers, connection=connection)
