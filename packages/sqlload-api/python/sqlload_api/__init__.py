"""SQLLoad's Python API for describing data and requests, without the engine.

A table function (or another tool's loader) yields what these build, and the engine that reads them runs it:

- `http(...)` jobs (`paginate`, `fan_out`) that make every request and parse every page in Rust;
- `json(...)` and `ndjson(...)` bytes, parsed without the GIL;
- `checkpoint(...)` positions between a logged table's rows.

Each crosses as plain data through a method the engine looks for (`__sqlload_http_job__`, `__sqlload_json__`,
`__sqlload_checkpoint__`), so this package holds no compiled code. `sqlload` re-exports it: `import sqlload as sl;
sl.http` is `sqlload_api.http`. Without a SQLLoad source, name the connection: `http(connection="shop_api")`.

A host that runs table functions on SQLLoad's engine (through the `sqlload-python-host` crate) builds each table's
`Context` with `Context.from_read(read)`, or a subclass of it with fields of its own; `sqlload_api._host` holds what every
host's crossing calls, so a process has one transform registry.
"""

from importlib.metadata import version

from sqlload_api._context import Connection, Context
from sqlload_api._data import Checkpoint, Field, JsonData, checkpoint, field, json, key, ndjson, parent
from sqlload_api._http import (
	CellName,
	Cells,
	ConnectionOwner,
	Http,
	Job,
	cells,
	cursor,
	header_row,
	http,
	link_header,
	listed,
	next_link,
	offset,
	page_number,
	single,
)
from sqlload_api._http_paths import literal_paths
from sqlload_api._transform import TransformContext, transform

__version__: str = version("sqlload-api")

__all__ = [
	"CellName",
	"Cells",
	"Checkpoint",
	"Connection",
	"ConnectionOwner",
	"Context",
	"Field",
	"Http",
	"Job",
	"JsonData",
	"TransformContext",
	"__version__",
	"cells",
	"checkpoint",
	"cursor",
	"field",
	"header_row",
	"http",
	"json",
	"key",
	"link_header",
	"listed",
	"literal_paths",
	"ndjson",
	"next_link",
	"offset",
	"page_number",
	"parent",
	"single",
	"transform",
]
