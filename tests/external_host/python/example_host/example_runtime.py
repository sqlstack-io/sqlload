"""The example host's runtime, which `sqlload-python-host`'s `ModuleHost` calls.

It declares tables its own way (`@model(source)`), passes data of its own through the engine (each source's `host`), and
builds each table's context as a subclass of `sqlload_api.Context`. Transforms, `sqlload_api.http` jobs and the
crossing are SQLLoad's, through `sqlload_api._host`.
"""

import importlib.util
import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import sqlload_api
from sqlload_api import _host

#: Each declared table: its source and function, by table name.
_tables: dict[str, tuple[str, Callable[["ExampleContext"], Any]]] = {}
#: The context of each table running, by table name.
_contexts: dict[str, "ExampleContext"] = {}


class ExampleContext(sqlload_api.Context):
	"""SQLLoad's context, with the model the host's own data names for the table."""

	def __init__(self, *, model: str, **fields: Any) -> None:
		super().__init__(**fields)
		self.model = model


def model(source: str) -> Callable[[Callable[[ExampleContext], Any]], Callable[[ExampleContext], Any]]:
	"""Declare the decorated function as a table of `source`, named after it."""

	def declare(function: Callable[[ExampleContext], Any]) -> Callable[[ExampleContext], Any]:
		_tables[function.__name__] = (source, function)
		return function

	return declare


def discover(root: str, python_paths: list[str]) -> str:
	"""Import the `.py` files in `python_paths` and describe their sources, each with the host's data: its tables' models."""
	root_path = Path(root or ".").resolve()
	_host.set_root(root_path)
	modules: dict[str, str] = {}
	for folder in python_paths:
		for path in sorted((root_path / folder).glob("*.py")):
			before = set(_tables)
			spec = importlib.util.spec_from_file_location(f"example_models_{path.stem}", path)
			assert spec is not None and spec.loader is not None
			spec.loader.exec_module(importlib.util.module_from_spec(spec))
			modules.update({name: str(path.relative_to(root_path)) for name in set(_tables) - before})
	sources: dict[str, dict[str, Any]] = {}
	for name, (source, _) in _tables.items():
		declared = sources.setdefault(source, {"name": source, "module": modules.get(name, ""), "settings": {"write_strategy": "replace", "tables": {}}, "host": {"models": {}}})
		declared["settings"]["tables"][name] = {"description": f"The {name} model."}
		declared["host"]["models"][name] = f"{source}.{name}"
	return json.dumps(list(sources.values()))


def start_table(request: str) -> Iterator[Any]:
	"""Call the table the read names with an `ExampleContext`; returns what it yields."""
	read = json.loads(request)
	_, function = _tables[read["table"]]
	ctx = ExampleContext.from_read(read, model=read["host"]["models"][read["table"]])
	_contexts[ctx.table] = ctx
	yielded = function(ctx)
	return iter([yielded]) if isinstance(yielded, sqlload_api.Job) else iter(yielded)


def current_state(table: str) -> str | None:
	return _host.current_state(_contexts.get(table))


def table_state(table: str) -> str | None:
	return _host.final_state(_contexts.pop(table, None))


def rename(name: str, columns: str | None, context: str) -> tuple[str | None, str | None]:
	return None, "the example host has no renames"


def finish_run() -> None:
	_contexts.clear()
