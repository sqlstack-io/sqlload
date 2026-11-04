"""The `sqlload` console script: the native command line, run in this Python process."""

import sys

from sqlload import _native


def main() -> None:
	"""Run `sqlload` with this process's arguments and exit with its code."""
	sys.stdout.flush()
	sys.stderr.flush()
	sys.exit(_native.run_cli(["sqlload", *sys.argv[1:]]))
