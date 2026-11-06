"""What a table function can yield besides rows: JSON bytes, which Rust parses without the GIL, and checkpoints."""

import json as _json
from typing import Any

from sqlload._native import JsonData

BytesLike = bytes | bytearray | memoryview | str


def json(data: BytesLike, path: str | None = None) -> JsonData:
	"""Rows in a JSON document: an array of objects (at dotted `path`, such as `"data.items"`), or objects."""
	return JsonData(_as_bytes(data), path=path)


def ndjson(data: BytesLike) -> JsonData:
	"""Rows as newline-delimited JSON, one object per line."""
	return JsonData(_as_bytes(data), lines=True)


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
	next run reads it as `ctx.state`."""
	return Checkpoint(state)
