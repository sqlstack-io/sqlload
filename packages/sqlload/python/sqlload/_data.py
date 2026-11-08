"""What a table function can yield besides rows: JSON bytes, which Rust parses without the GIL, and checkpoints."""

import json as _json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from sqlload._native import JsonData

BytesLike = bytes | bytearray | memoryview | str


@dataclass(frozen=True)
class Field:
	"""One projected column's path, and whether every row must have a value there."""

	path: str
	required: bool = False


#: `fields=`: each column's path, such as `"$.address.city"`, or an `sl.field(...)`.
Fields = Mapping[str, "str | Field"]


def field(path: str, *, required: bool = False) -> Field:
	"""The value at `path` (such as `"$.updated-at"`) for `fields=`; `required=True` fails the table on a row without one."""
	return Field(path, required)


def json(data: BytesLike, path: str | None = None, *, fields: Fields | None = None, keep_record: str | None = None) -> JsonData:
	"""Rows in a JSON document: an array of objects (at dotted `path`, such as `"data.items"`), or objects.

	`fields` loads only those values, renamed (`{"account_code": "$.code"}`); `keep_record` adds each row's whole
	object as a JSON column."""
	return JsonData(_as_bytes(data), path=path, fields=field_list(fields), keep_record=keep_record)


def ndjson(data: BytesLike, *, fields: Fields | None = None, keep_record: str | None = None) -> JsonData:
	"""Rows as newline-delimited JSON, one object per line; `fields` and `keep_record` as for `sl.json`."""
	return JsonData(_as_bytes(data), lines=True, fields=field_list(fields), keep_record=keep_record)


def field_list(fields: Fields | None) -> list[tuple[str, str, bool]] | None:
	"""`fields` as (column, path, required) triples, in the order given."""
	if fields is None:
		return None
	if not isinstance(fields, Mapping):
		raise TypeError(f"fields takes a mapping of column name to path, such as {{\"id\": \"$.id\"}}, not {fields!r}")
	listed = []
	for name, spec in fields.items():
		projected = spec if isinstance(spec, Field) else Field(spec)
		if not isinstance(name, str):
			raise TypeError(f"fields names columns with text, not {name!r}")
		if not isinstance(projected.path, str):
			raise TypeError(f"fields[{name!r}] takes a path such as \"$.id\" or sl.field(...), not {spec!r}")
		if not isinstance(projected.required, bool):
			raise TypeError(f"fields[{name!r}]: required takes True or False, not {projected.required!r}")
		listed.append((name, projected.path, projected.required))
	return listed


def _as_bytes(data: BytesLike) -> bytes | bytearray:
	if isinstance(data, str):
		return data.encode()
	if isinstance(data, memoryview):
		return data.tobytes()
	return data


class Checkpoint:
	"""A position yielded among a logged table's rows; see `checkpoint`."""

	def __init__(self, state: Any) -> None:
		try:
			self._state = _json.dumps(state)
		except (TypeError, ValueError) as error:
			raise TypeError(f"sl.checkpoint state is not JSON: {error}") from error

	def __sqlload_checkpoint__(self) -> str:
		return self._state

	def __repr__(self) -> str:
		return f"sl.checkpoint({self._state})"


def checkpoint(state: Any) -> Checkpoint:
	"""The position (any JSON value) that covers exactly the rows yielded before it, for a `state="logged"`
	table: `yield sl.checkpoint(page.next_token)` after a page's rows. A commit records the latest checkpoint
	whose rows are all in it, so a run that fails part-way resumes there without loading any row twice; the
	next run reads it as `ctx.state`. Rows yielded after the last checkpoint are loaded again by the next run
	unless the function leaves the final position in `ctx.state`; `sl.checkpoint(None)` clears the position."""
	return Checkpoint(state)
