"""What a table function can yield besides rows: JSON bytes, which Rust parses without the GIL, and checkpoints."""

import json as _json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from sqlload._native import JsonData

BytesLike = bytes | bytearray | memoryview | str


#: The conversions `sl.field(parse=...)` knows, a closed list; anything else belongs downstream or in a Python table.
EPOCH_UNITS = ("epoch_s", "epoch_ms", "epoch_us")
FORMAT_KINDS = ("timestamp", "date")


@dataclass(frozen=True)
class Field:
	"""One projected column's path, whether every row must have a value there, and how that value converts."""

	path: str
	required: bool = False
	parse: str | Mapping[str, str] | None = None
	on_error: str = "fail"
	#: Where `path` starts when not at the row: `sl.parent`'s enclosing object or `sl.key`'s request key, as the spec's `from`.
	source: Mapping[str, Any] | None = None


#: `fields=`: each column's path, such as `"$.address.city"`, or an `sl.field(...)`.
Fields = Mapping[str, "str | Field"]


def field(path: str, *, required: bool = False, parse: str | Mapping[str, str] | None = None, on_error: str = "fail") -> Field:
	"""The value at `path` (such as `"$.updated-at"`) for `fields=`; `required=True` fails the table on a row without one.

	`parse` converts the value: `"epoch_s"`, `"epoch_ms"` or `"epoch_us"` turn a number (or numeric text) into a UTC
	timestamp; `{"timestamp": "%d/%m/%Y %H:%M"}` and `{"date": "%d/%m/%Y"}` read text with a strftime format. A value
	that doesn't convert fails the table, naming the field and the value, or loads NULL with `on_error="null"`."""
	return Field(path, required, parse, on_error)


def parent(path: str, *, levels: int = 1, required: bool = False, parse: str | Mapping[str, str] | None = None, on_error: str = "fail") -> Field:
	"""In an `sl.http` job's `fields=`, the value at `path` in the object around each row that `select` passes through:
	the nearest with `levels=1`, further out with more, the response document itself outermost (so a page's envelope
	works too). `required`, `parse` and `on_error` are as for `sl.field`; `plan` refuses more levels than `select` has."""
	if isinstance(levels, bool) or not isinstance(levels, int):
		raise TypeError(f"sl.parent levels takes a whole number, not {levels!r}")
	return Field(path, required, parse, on_error, {"kind": "parent", "levels": levels})


def key(name: str, *, required: bool = False) -> Field:
	"""In a `fan_out`'s `fields=`, the value of key field `name` its request was made for, from any level above
	(each key keeps the fields of the keys above it). `plan` refuses a name no level provides, and `batch=`, where
	one request carries many keys."""
	if not isinstance(name, str):
		raise TypeError(f"sl.key takes a key field's name, not {name!r}")
	return Field("$", required, None, "fail", {"kind": "key", "name": name})


def json(data: BytesLike, path: str | None = None, *, fields: Fields | None = None, keep_record: str | None = None) -> JsonData:
	"""Rows in a JSON document: an array of objects (at dotted `path`, such as `"data.items"`), or objects.

	`fields` loads only those values, renamed (`{"account_code": "$.code"}`); `keep_record` adds each row's whole
	object as a JSON column."""
	return JsonData(_as_bytes(data), path=path, fields=_fields_text(fields), keep_record=keep_record)


def ndjson(data: BytesLike, *, fields: Fields | None = None, keep_record: str | None = None) -> JsonData:
	"""Rows as newline-delimited JSON, one object per line; `fields` and `keep_record` as for `sl.json`."""
	return JsonData(_as_bytes(data), lines=True, fields=_fields_text(fields), keep_record=keep_record)


def field_list(fields: Fields | None) -> list[dict[str, Any]] | None:
	"""`fields` as the specs Rust reads (name, path, required, and parse and on_error when set), in the order given."""
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
		source = {} if projected.source is None else {"from": dict(projected.source)}
		listed.append({"name": name, "path": projected.path, "required": projected.required, **_conversion(name, projected), **source})
	return listed


def _conversion(name: str, projected: Field) -> dict[str, Any]:
	"""`projected`'s parse and on_error as spec keys, left out when unset so specs without them are unchanged."""
	if projected.on_error not in ("fail", "null"):
		raise ValueError(f'fields[{name!r}]: on_error must be "fail" or "null", not {projected.on_error!r}')
	parse = projected.parse
	if parse is None:
		if projected.on_error != "fail":
			raise ValueError(f"fields[{name!r}]: on_error applies to values parse converts; set parse too")
		return {}
	if isinstance(parse, str) and parse in EPOCH_UNITS:
		kind: dict[str, Any] = {"kind": parse}
	elif isinstance(parse, Mapping) and len(parse) == 1 and next(iter(parse)) in FORMAT_KINDS and isinstance(next(iter(parse.values())), str):
		((kind_name, format_text),) = parse.items()
		kind = {"kind": kind_name, "format": format_text}
	else:
		raise ValueError(
			f'fields[{name!r}]: parse takes "epoch_s", "epoch_ms", "epoch_us", {{"timestamp": format}} or {{"date": format}}, not {parse!r}; '
			"other conversions belong downstream or in a Python table"
		)
	return {"parse": kind, **({"on_error": "null"} if projected.on_error == "null" else {})}


def _fields_text(fields: Fields | None) -> str | None:
	"""`fields` as the JSON list of specs `JsonData` reads."""
	listed = field_list(fields)
	for spec in listed or []:
		if "from" in spec:
			raise ValueError(f"fields[{spec['name']!r}]: sl.parent and sl.key read around the rows of an sl.http job; sl.json and sl.ndjson rows have no request")
	return None if listed is None else _json.dumps(listed)


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
