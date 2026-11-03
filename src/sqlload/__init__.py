"""SQLLoad: export Kafka topics to Snowflake.

The exporter is the native `sqlload` executable from the `sqlload-cli` package; this package
installs it and locates it for Python callers.
"""

import os
import shutil
import sysconfig
from importlib.metadata import version

__version__: str = version("sqlload")


def executable() -> str:
	"""The path of the `sqlload` executable installed alongside this package, else the one on PATH."""
	name: str = "sqlload.exe" if os.name == "nt" else "sqlload"
	installed: str = os.path.join(sysconfig.get_path("scripts"), name)
	if os.path.isfile(installed):
		return installed
	found: str | None = shutil.which("sqlload")
	if found is None:
		raise FileNotFoundError("the sqlload executable is missing; reinstall the sqlload package")
	return found
