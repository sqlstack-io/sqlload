"""SQLLoad's MotherDuck bridge: SQL run on PyPI's duckdb (1.4.1 to 1.5.6, which MotherDuck accepts) for a SQLLoad process, whose bundled DuckDB MotherDuck doesn't accept yet.

SQLLoad starts it as `python -c <this file>` and sends one JSON request per line on stdin; each reply is one JSON line on stdout, tagged with its request's id:

    {"id": 1, "op": "open", "database": "md:shop?motherduck_token=...", "config": {...}}
        -> {"id": 1, "ok": true, "duckdb": "1.5.6", "python": "3.12.3"}
    {"id": 2, "op": "run", "cursor": 3, "sql": "SELECT ...", "params": ["raw"]}
        -> {"id": 2, "ok": true, "rows": [["id", "BIGINT"], ...]}
    {"op": "close", "cursor": 3}
        -> no reply

A failure replies {"id": n, "ok": false, "error": "...", "type": "CatalogException"}. Each cursor is a connection of its own to
the opened database (DuckDB's `cursor()`), created on its first request and run on its own thread in request order, so one
table's transaction never mixes with another's. The bridge exits when stdin closes, which rolls back any open transaction.
"""

import sys

# `python -c` puts the working directory first on the path, where a stray duckdb.py would shadow the real package.
if sys.path and sys.path[0] == "":
	del sys.path[0]

import json
import math
import os
import queue
import signal
import threading


def plain(value):
	"""`value` as JSON can carry it: numbers, text, booleans and NULL as they are, lists item by item, anything else as its text."""
	if value is None or isinstance(value, (bool, int, str)):
		return value
	if isinstance(value, float):
		return value if math.isfinite(value) else str(value)
	if isinstance(value, (list, tuple)):
		return [plain(item) for item in value]
	return str(value)


def main():
	# Replies go to a private copy of stdout; anything else that prints (DuckDB, an extension) lands on stderr instead.
	replies = os.fdopen(os.dup(1), "wb", buffering=0)
	os.dup2(2, 1)
	sys.stdout = sys.stderr
	# Ctrl-C reaches the whole process group; SQLLoad ends, its pipe closes, and that ends the bridge.
	signal.signal(signal.SIGINT, signal.SIG_IGN)
	lock = threading.Lock()

	def reply(message):
		line = json.dumps(message, ensure_ascii=True, allow_nan=False).encode("ascii") + b"\n"
		with lock:
			replies.write(line)

	def failed(request, error):
		reply({"id": request.get("id"), "ok": False, "error": str(error), "type": type(error).__name__})

	database = None
	cursors = {}

	def serve(work):
		cursor = None
		while True:
			request = work.get()
			if request is None:
				break
			try:
				if cursor is None:
					cursor = database.cursor()
				cursor.execute(request["sql"], request.get("params") or None)
				rows = cursor.fetchall() if cursor.description else []
				reply({"id": request["id"], "ok": True, "rows": [[plain(value) for value in row] for row in rows]})
			except BaseException as error:  # noqa: BLE001 - every failure goes back to SQLLoad as its reply, or it would wait forever
				failed(request, error)
		if cursor is not None:
			cursor.close()

	for line in sys.stdin.buffer:
		request = json.loads(line)
		op = request.get("op")
		if op == "open":
			try:
				import duckdb

				database = duckdb.connect(request["database"], config=request.get("config") or {})
				reply({"id": request["id"], "ok": True, "duckdb": duckdb.__version__, "python": sys.version.split()[0]})
			except Exception as error:  # noqa: BLE001
				failed(request, error)
		elif op == "run":
			work = cursors.get(request["cursor"])
			if work is None:
				work = cursors[request["cursor"]] = queue.Queue()
				threading.Thread(target=serve, args=(work,), daemon=True).start()
			work.put(request)
		elif op == "close":
			work = cursors.pop(request["cursor"], None)
			if work is not None:
				work.put(None)
		else:
			failed(request, ValueError(f"unknown op {op!r}"))
	# stdin closed: SQLLoad is done or gone. Statements still running are abandoned, and open transactions roll back.
	replies.flush()
	os._exit(0)


main()
