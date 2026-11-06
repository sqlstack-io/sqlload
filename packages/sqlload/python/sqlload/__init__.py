"""SQLLoad: load data into warehouses and files.

The `sqlload` command runs the native loader in-process (`sqlload._native`), so Python sources
declared here run alongside it:

```python
import sqlload as sl

shop = sl.source("shop", connection="shop", destination="prod", write_strategy="merge")


@shop.table(unique_key="order_id")
def orders(ctx: sl.Context):
    '''Orders from the shop API, one row per order.'''
    yield from ctx.client.get("/orders").json()["items"]
```

The table loads into `shop__orders` (`{source}__{table}`, or the `table_name` the source or table
sets).

A table function yields dicts, lists of dicts, `sl.json(...)` / `sl.ndjson(...)` bytes, or Arrow
data (pyarrow, polars, pandas); SQLLoad detects which. A `state="logged"` table can yield
`sl.checkpoint(position)` between them.
"""

import os
import shutil
import sysconfig
from importlib.metadata import version

from sqlload._data import Checkpoint, checkpoint, json, ndjson
from sqlload._declare import Context, Source, Table, source, table

__version__: str = version("sqlload")

__all__ = [
	"Checkpoint",
	"Context",
	"Source",
	"Table",
	"__version__",
	"checkpoint",
	"executable",
	"json",
	"ndjson",
	"source",
	"table",
]


def executable() -> str:
	"""The path of the `sqlload` command installed alongside this package, else the one on PATH."""
	name: str = "sqlload.exe" if os.name == "nt" else "sqlload"
	installed: str = os.path.join(sysconfig.get_path("scripts"), name)
	if os.path.isfile(installed):
		return installed
	found: str | None = shutil.which("sqlload")
	if found is None:
		raise FileNotFoundError("the sqlload command is missing; reinstall the sqlload package")
	return found
