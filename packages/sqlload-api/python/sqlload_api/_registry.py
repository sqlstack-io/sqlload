"""Functions a project picks by name: each decorator (`@sl.transform`) registers functions in a registry of its own.

A function registers under its own name, or the one the decorator is given; `sqlload.toml` names it. Discovery imports
every module under `python_paths`, which runs the decorators, so `plan` can look a name up and refuse one that is
unknown or registered twice.
"""

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Registered:
	"""A function registered under a name, and the file it is defined in."""

	function: Callable[..., Any]
	file: str


class Registry:
	"""The functions one decorator registered, by name."""

	def __init__(self, decorator: str) -> None:
		self.decorator = decorator
		self._named: dict[str, list[Registered]] = {}
		#: Functions passed as themselves (`transform=page`) rather than by a registered name, by the key they go by, with
		#: the name they are shown by. Held until the run finishes (`release`): a job keeps only the key, so an inline lambda
		#: would otherwise be gone before its pages are called.
		self._passed: dict[str, tuple[Callable[..., Any], str]] = {}

	def register(self, name: str, function: Callable[..., Any]) -> None:
		"""Register `function` under `name`; the same function registered again (its module imported again) replaces itself."""
		if not callable(function):
			raise TypeError(f"{self.decorator} decorates a function, not {type(function).__name__}")
		if not name:
			raise ValueError(f"{self.decorator} needs a name that is not empty")
		file = _file(function)
		kept = [entry for entry in self._named.get(name, []) if (entry.file, entry.function.__qualname__) != (file, function.__qualname__)]
		self._named[name] = [*kept, Registered(function, file)]

	def key(self, function: Callable[..., Any] | str) -> tuple[str, str]:
		"""The key a function goes by, and the name it is shown by: a registered name as it is; a function itself, a key
		of its own that resolves to that very function (even when another function is registered under its name), shown
		by the name it is registered under, else its qualified name."""
		if isinstance(function, str):
			return function, function
		if not callable(function):
			raise TypeError(f"expected a function or the name {self.decorator} registered one under, not {type(function).__name__}")
		registered = (name for name, entries in self._named.items() if any(entry.function is function for entry in entries))
		shown = next(registered, getattr(function, "__qualname__", type(function).__name__))
		key = f"{shown}#{id(function):x}"
		self._passed[key] = (function, shown)
		return key, shown

	def release(self) -> None:
		"""Let go of the functions passed as themselves, once the run that passed them finished."""
		self._passed.clear()

	def lookup(self, key: str, root: Path | None = None) -> tuple[Callable[..., Any] | None, str | None]:
		"""The function registered under `key` (or passed as itself), or why there is none: no function has the name (the
		registered ones listed), or two have it (their files named, relative to `root`)."""
		if key in self._passed:
			return self._passed[key][0], None
		entries = self._named.get(key, [])
		if len(entries) == 1:
			return entries[0].function, None
		if entries:
			files = " and ".join(f"{_shown(entry.file, root)} ({entry.function.__qualname__})" for entry in entries)
			return None, f'{len(entries)} functions are registered as {key}, in {files}; give one another name with {self.decorator}("...")'
		names = ", ".join(sorted(self._named))
		if not names:
			return None, f'no function is registered as {key}: decorate one with {self.decorator}, or {self.decorator}("{key}") to give it that name'
		return None, f"no function is registered as {key} with {self.decorator}; the registered ones are {names}"

	def shown(self, key: str) -> str:
		"""The name `key` is shown by in errors: a passed function's, else the key itself."""
		passed = self._passed.get(key)
		return passed[1] if passed is not None else key

	def source_file(self, key: str) -> str | None:
		"""The file the function `key` names is defined in."""
		function, _ = self.lookup(key)
		return _file(function) if function is not None else None


def _file(function: Callable[..., Any]) -> str:
	"""The file `function` is defined in, resolved; empty when Python can't tell."""
	try:
		path = inspect.getsourcefile(function) or inspect.getfile(function)
	except TypeError:
		return ""
	return str(Path(path).resolve())


def _shown(file: str, root: Path | None) -> str:
	"""`file` relative to the project folder `root` when it is under it."""
	path = Path(file)
	if root is not None and root in path.parents:
		return path.relative_to(root).as_posix()
	return file
