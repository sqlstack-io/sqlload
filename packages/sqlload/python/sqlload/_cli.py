"""The `sqlload` console script: the native command line, run in this Python process."""

import contextlib
import ctypes
import os
import signal
import sys
import threading
from collections.abc import Callable, Mapping

from sqlload import _native

#: Set in the environment of the process `main` starts again with libstdc++ preloaded, so it starts again at most once.
PRELOADED = "SQLLOAD_PRELOADED"
#: Set to any value to keep `main` from preloading libstdc++.
NO_PRELOAD = "SQLLOAD_NO_PRELOAD"
LIBSTDCXX = "libstdc++.so.6"


def libstdcxx() -> str | None:
	"""Where libstdc++ is, by the path this process loaded it from, else its soname when the loader finds it; None when there is none."""
	try:
		ctypes.CDLL(LIBSTDCXX)
	except OSError:
		return None
	try:
		with open("/proc/self/maps") as maps:
			for line in maps:
				path = line.split(maxsplit=5)[-1].strip()
				if path.startswith("/") and os.path.basename(path).startswith(LIBSTDCXX):
					# LD_PRELOAD splits on spaces and colons, so a path holding either goes by its soname.
					return LIBSTDCXX if " " in path or ":" in path else path
	except OSError:
		pass
	return LIBSTDCXX


def preloaded_environment(
	environ: Mapping[str, str], platform: str, find: Callable[[], str | None] = libstdcxx
) -> dict[str, str] | None:
	"""The environment to start `sqlload` again in on Linux, with libstdc++ preloaded: MySQL Connector/ODBC's driver, built for
	static thread-local storage, loads in a Python process only when libstdc++ was there from its start. None when this process
	already started again, `SQLLOAD_NO_PRELOAD` is set, libstdc++ is preloaded already or isn't installed, or not on Linux."""
	preload = environ.get("LD_PRELOAD", "")
	if not platform.startswith("linux") or environ.get(PRELOADED) or environ.get(NO_PRELOAD) or "libstdc++" in preload:
		return None
	library = find()
	if library is None:
		return None
	return {**environ, PRELOADED: "1", "LD_PRELOAD": f"{library} {preload}".strip()}


def main() -> None:
	"""Run `sqlload` with this process's arguments and exit with its code; on Linux, in a process started again once with libstdc++ preloaded."""
	environ = preloaded_environment(os.environ, sys.platform)
	# A script read from stdin (argv[0] "-" or "") can't be read again by a second start.
	if environ is not None and sys.executable and sys.argv[0] not in ("", "-"):
		sys.stdout.flush()
		sys.stderr.flush()
		# A start that fails goes on in this process, as it would without the preload.
		with contextlib.suppress(OSError, ValueError):
			os.execve(sys.executable, sys.orig_argv, environ)
	if threading.current_thread() is threading.main_thread():
		# Python's handler only sets a flag, which the native command never checks while it runs; Ctrl-C ends it as it ends the binary.
		signal.signal(signal.SIGINT, signal.SIG_DFL)
	# A MotherDuck target runs its SQL on this Python's duckdb package (the sqlload[motherduck] extra), in a process of its own.
	if not os.environ.get("SQLLOAD_MOTHERDUCK_PYTHON") and sys.executable:
		os.environ["SQLLOAD_MOTHERDUCK_PYTHON"] = sys.executable
	sys.stdout.flush()
	sys.stderr.flush()
	args = ["sqlload", *sys.argv[1:]]
	if "--json" not in args:
		sys.exit(_native.run_cli(args))
	# The native command writes its one JSON document to the process's stdout; a source's print() goes to stderr, so stdout still parses.
	with contextlib.redirect_stdout(sys.stderr):
		code = _native.run_cli(args)
	sys.exit(code)
