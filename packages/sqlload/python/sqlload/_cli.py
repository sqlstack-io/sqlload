"""The `sqlload` console script: the native command line, run in this Python process."""

import signal
import sys
import threading

from sqlload import _native


def main() -> None:
	"""Run `sqlload` with this process's arguments and exit with its code."""
	if threading.current_thread() is threading.main_thread():
		# Python's handler only sets a flag, which the native command never checks while it runs; Ctrl-C ends it as it ends the binary.
		signal.signal(signal.SIGINT, signal.SIG_DFL)
	sys.stdout.flush()
	sys.stderr.flush()
	sys.exit(_native.run_cli(["sqlload", *sys.argv[1:]]))
