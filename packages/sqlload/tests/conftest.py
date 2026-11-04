"""Shared fixtures for the sqlload package tests."""

from pathlib import Path

import pytest

PROJECT = """\
[connections.wh]
kind = "duckdb"
path = ":memory:"

[connections.app_pg]
kind = "postgres"
host = "localhost"
user = "loader"
database = "app"

[targets.dev]
connection = "wh"
loader_schema = "RAW"

[sources.app]
kind = "postgres"
connection = "app_pg"
destination = "dev"
include = ["public.*"]
"""


@pytest.fixture
def project(tmp_path: Path) -> Path:
	"""A small loader project: a Postgres source loading into in-memory DuckDB."""
	path = tmp_path / "sqlload.toml"
	path.write_text(PROJECT)
	return path
