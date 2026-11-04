"""JSON bytes a table function can yield; Rust parses them without the GIL."""

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
