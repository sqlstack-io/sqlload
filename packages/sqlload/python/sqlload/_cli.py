"""The `sqlload` console script: the native command line, run in this Python process."""

import contextlib
import os
import signal
import sys
import threading

from sqlload import _native


def main() -> None:
	"""Run `sqlload` with this process's arguments and exit with its code."""
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
