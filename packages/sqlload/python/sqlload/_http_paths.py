"""The `sl.http` field paths a table function writes as literals, read from its source without calling it.

`plan` checks them with load's own path checker, so a bad literal path fails `plan` instead of the load. Only calls whose
names resolve to sqlload count: `paginate` and `fan_out` on an `sl.http(...)` client, and `sl.field`, `sl.parent` and
`sl.key` in their `fields=`. A path `plan` can't read as a literal (a variable, an f-string, a dict built elsewhere), or a
function whose source can't be read, is left to load, which checks every job before its first request.
"""

import ast
import inspect
import json
import sys
import textwrap
import types
from collections.abc import Callable, Iterator, Mapping
from typing import Any

from sqlload import _data, _http

#: The methods of an `sl.http` client that build a job.
JOB_METHODS: frozenset[str] = frozenset({"paginate", "fan_out"})

#: The module a name must resolve into for its call to count.
PACKAGE = "sqlload"

#: A name or attribute that doesn't resolve to anything `plan` may look at.
_UNRESOLVED: Any = object()

#: A local name assigned an `sl.http(...)` client inside the table function.
_LOCAL_CLIENT: Any = object()


def literal_paths(function: Callable[..., Any]) -> dict[str, Any] | None:
	"""The literal paths of the jobs `function` builds that load rows: each `fields=` entry's `[name, path]` and each
	`key_path`; what `plan` can't read as a literal; and whether the source can't be read at all. A job passed as another
	job's `over=` is left out: load refuses its `fields` and `key_path` whatever their paths. `None` when the function builds
	no job `plan` can see."""
	try:
		tree = ast.parse(textwrap.dedent(inspect.getsource(function)))
	except (OSError, TypeError, SyntaxError):
		return {"fields": [], "key_paths": [], "computed": [], "no_source": True}
	definition = next((node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))), None)
	if definition is None:
		return {"fields": [], "key_paths": [], "computed": [], "no_source": True}
	names = _Names(function, definition)
	jobs = [call for call in ast.walk(definition) if isinstance(call, ast.Call) and _job_method(call, names)]
	if not jobs:
		return None
	found: dict[str, Any] = {"fields": [], "key_paths": [], "computed": [], "no_source": False}
	over = _over_jobs(definition, jobs)
	for call in jobs:
		if id(call) not in over:
			_read_call(call, names, found)
	return found


class _Names:
	"""What the names a table function uses resolve to: its own imports and `sl.http(...)` clients, else its closure and
	module globals; a name it assigns otherwise shadows them and resolves to nothing."""

	def __init__(self, function: Callable[..., Any], definition: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
		self.outer: dict[str, Any] = dict(getattr(function, "__globals__", {}))
		code = getattr(function, "__code__", None)
		for name, cell in zip(code.co_freevars if code else (), getattr(function, "__closure__", None) or ()):
			try:
				self.outer[name] = cell.cell_contents
			except ValueError:
				self.outer[name] = _UNRESOLVED
		self.local: dict[str, Any] = {}
		arguments = definition.args
		for argument in [*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs, arguments.vararg, arguments.kwarg]:
			if argument is not None:
				self.local[argument.arg] = _UNRESOLVED
		for node in ast.walk(definition):
			self._bind(node)

	def _bind(self, node: ast.AST) -> None:
		if isinstance(node, ast.Import):
			for alias in node.names:
				bound = alias.asname or alias.name.partition(".")[0]
				self.local[bound] = sys.modules.get(alias.name if alias.asname else bound, _UNRESOLVED)
		elif isinstance(node, ast.ImportFrom):
			module = sys.modules.get(node.module or "") if node.level == 0 else None
			for alias in node.names:
				self.local[alias.asname or alias.name] = getattr(module, alias.name, _UNRESOLVED) if _ours(module) else _UNRESOLVED
		elif isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
			client = isinstance(node.value, ast.Call) and self.resolve(node.value.func) in (_http.http, _http.Http)
			self.local[node.targets[0].id] = _LOCAL_CLIENT if client and self.local.get(node.targets[0].id, _LOCAL_CLIENT) is _LOCAL_CLIENT else _UNRESOLVED
		elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store) and node.id not in self.local:
			self.local[node.id] = _UNRESOLVED

	def resolve(self, node: ast.expr) -> Any:
		"""What a name or a dotted attribute resolves to, looking only into sqlload's modules and clients."""
		if isinstance(node, ast.Name):
			return self.local[node.id] if node.id in self.local else self.outer.get(node.id, _UNRESOLVED)
		if not isinstance(node, ast.Attribute):
			return _UNRESOLVED
		owner = self.resolve(node.value)
		if isinstance(owner, types.ModuleType) and _ours(owner):
			return getattr(owner, node.attr, _UNRESOLVED)
		if (owner is _LOCAL_CLIENT or isinstance(owner, _http.Http)) and node.attr in JOB_METHODS:
			return node.attr
		return _UNRESOLVED


def _ours(module: Any) -> bool:
	"""Whether `module` is sqlload or one of its modules."""
	name = getattr(module, "__name__", "") if isinstance(module, types.ModuleType) else ""
	return name == PACKAGE or name.startswith(f"{PACKAGE}.")


def _job_method(call: ast.Call, names: _Names) -> str | None:
	"""`paginate` or `fan_out` when `call` builds a job on an `sl.http` client."""
	method = names.resolve(call.func)
	return method if isinstance(method, str) else None


def _over_jobs(definition: ast.AST, jobs: list[ast.Call]) -> set[int]:
	"""The jobs passed as another job's `over=`, directly or through a name assigned them."""
	assigned: dict[str, list[int]] = {}
	for node in ast.walk(definition):
		if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and isinstance(node.value, ast.Call):
			assigned.setdefault(node.targets[0].id, []).append(id(node.value))
	over: set[int] = set()
	for call in jobs:
		for keyword in call.keywords:
			if keyword.arg != "over":
				continue
			if isinstance(keyword.value, ast.Call):
				over.add(id(keyword.value))
			elif isinstance(keyword.value, ast.Name):
				over.update(assigned.get(keyword.value.id, []))
	return over


def _read_call(call: ast.Call, names: _Names, found: dict[str, Any]) -> None:
	"""Add `call`'s literal `fields=` and `key_path=` paths to `found`, or what it can't read as literals."""
	for keyword in call.keywords:
		if keyword.arg is None:
			found["computed"].append(f"{_job_method(call, names)}(**...)")
		elif keyword.arg == "fields":
			_read_fields(keyword.value, names, found)
		elif keyword.arg == "key_path":
			path = _literal_path(keyword.value, names, (_data.parent,))
			_record(found, "key_path", path, lambda text: found["key_paths"].append(text))


def _read_fields(value: ast.expr, names: _Names, found: dict[str, Any]) -> None:
	if isinstance(value, ast.Constant) and value.value is None:
		return
	if not isinstance(value, ast.Dict):
		found["computed"].append("fields")
		return
	for key, spec in zip(value.keys, value.values):
		if not (isinstance(key, ast.Constant) and isinstance(key.value, str)):
			found["computed"].append("fields")
			continue
		name = key.value
		if isinstance(spec, ast.Call) and names.resolve(spec.func) is _data.key:
			continue
		path = _literal_path(spec, names, (_data.field, _data.parent))
		_record(found, f"fields[{json.dumps(name)}]", path, lambda text, name=name: found["fields"].append([name, text]))


def _literal_path(value: ast.expr, names: _Names, functions: tuple[Callable[..., Any], ...]) -> str | None:
	"""The path `value` writes as a literal: a string, or the first argument (or `path=`) of one of sqlload's `functions`."""
	if isinstance(value, ast.Constant) and isinstance(value.value, str):
		return value.value
	if not (isinstance(value, ast.Call) and names.resolve(value.func) in functions):
		return None
	given = [*value.args[:1], *(keyword.value for keyword in value.keywords if keyword.arg == "path")]
	return next(_strings(given), None)


def _strings(values: list[ast.expr]) -> Iterator[str]:
	for value in values:
		if isinstance(value, ast.Constant) and isinstance(value.value, str):
			yield value.value


def _record(found: Mapping[str, Any], label: str, path: str | None, add: Callable[[str], None]) -> None:
	if path is None:
		found["computed"].append(label)
	else:
		add(path)
