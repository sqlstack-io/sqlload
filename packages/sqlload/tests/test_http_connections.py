"""How sl.http jobs use connections: cursor windows share one pool of HTTPS connections, and read_ahead ramps up instead of
sending every request at once. The stub API serves over TLS with a certificate from a CA made for the test, and counts the
TLS handshakes and requests it sees."""

import json
import os
import ssl
import subprocess
import sys
import textwrap
import threading
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import duckdb
import pytest

DAYS = 12
ROWS_PER_DAY = 5
#: Midnight UTC twelve days ago: the first window's start, so the windows of one day each end at today's.
START = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=DAYS)
ROWS = [{"id": n + 1, "updated_at": (START + timedelta(days=1) / ROWS_PER_DAY * n).isoformat()} for n in range(DAYS * ROWS_PER_DAY)]


class Api(BaseHTTPRequestHandler):
	"""`/items?updated_from&updated_to&offset&limit`: the rows in that range, `limit` from `offset`; without a range, every row."""

	protocol_version = "HTTP/1.1"

	def log_message(self, *args: Any) -> None:
		pass

	def do_GET(self) -> None:
		query = {name: values[0] for name, values in parse_qs(urlparse(self.path).query).items()}
		low = datetime.fromisoformat(query["updated_from"]) if "updated_from" in query else START
		high = datetime.fromisoformat(query["updated_to"]) if "updated_to" in query else START + timedelta(days=DAYS)
		ranged = [row for row in ROWS if low.astimezone(timezone.utc) <= datetime.fromisoformat(row["updated_at"]) < high.astimezone(timezone.utc)]
		offset, limit = int(query.get("offset", 0)), int(query["limit"])
		with self.server.lock:  # type: ignore[attr-defined]
			self.server.requests += 1  # type: ignore[attr-defined]
		data = json.dumps({"items": ranged[offset : offset + limit]}).encode()
		self.send_response(200)
		self.send_header("Content-Type", "application/json")
		self.send_header("Content-Length", str(len(data)))
		self.end_headers()
		self.wfile.write(data)


class TlsServer(ThreadingHTTPServer):
	"""Serves `Api` over TLS, each connection's handshake in its own thread, counting the handshakes that succeed."""

	daemon_threads = True

	def finish_request(self, request: Any, client_address: Any) -> None:
		try:
			tls = self.context.wrap_socket(request, server_side=True)  # type: ignore[attr-defined]
		except (ssl.SSLError, OSError):
			return
		with self.lock:  # type: ignore[attr-defined]
			self.handshakes += 1  # type: ignore[attr-defined]
		with tls:
			super().finish_request(tls, client_address)


@pytest.fixture(scope="module")
def certificates(tmp_path_factory: pytest.TempPathFactory) -> Path:
	"""A CA, and a certificate it signed for `localhost` with its key."""
	folder = tmp_path_factory.mktemp("tls")

	def openssl(*args: str) -> None:
		subprocess.run(["openssl", *args], cwd=folder, check=True, capture_output=True)

	openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", "ca.key", "-out", "ca.pem", "-days", "2", "-subj", "/CN=test CA",
		"-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign")
	openssl("req", "-newkey", "rsa:2048", "-nodes", "-keyout", "server.key", "-out", "server.csr", "-subj", "/CN=localhost")
	(folder / "ext.cnf").write_text("subjectAltName=DNS:localhost\nbasicConstraints=CA:FALSE\nextendedKeyUsage=serverAuth\n")
	openssl("x509", "-req", "-in", "server.csr", "-CA", "ca.pem", "-CAkey", "ca.key", "-CAcreateserial", "-out", "server.pem", "-days", "2", "-extfile", "ext.cnf")
	return folder


@pytest.fixture
def api(certificates: Path) -> Iterator[TlsServer]:
	server = TlsServer(("127.0.0.1", 0), Api)
	server.context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)  # type: ignore[attr-defined]
	server.context.load_cert_chain(certificates / "server.pem", certificates / "server.key")  # type: ignore[attr-defined]
	server.lock, server.handshakes, server.requests = threading.Lock(), 0, 0  # type: ignore[attr-defined]
	threading.Thread(target=server.serve_forever, daemon=True).start()
	yield server
	server.shutdown()


def load(tmp_path: Path, api: TlsServer, certificates: Path, source: str, job: str) -> subprocess.CompletedProcess[str]:
	"""Load table `items` of source `source` (its `sl.source` arguments), returning `job`, from the stub into DuckDB, trusting the test's CA."""
	port = api.server_address[1]
	(tmp_path / "sources").mkdir()
	(tmp_path / "home").mkdir()
	(tmp_path / "sqlload.toml").write_text(
		textwrap.dedent(f"""\
			python_paths = ["sources"]

			[connections.wh]
			kind = "duckdb"
			database = "wh.duckdb"

			[connections.api]
			kind = "http"
			base_url = "https://localhost:{port}"

			[targets.dev]
			connection = "wh"
			loader_schema = "raw"
		""")
	)
	(tmp_path / "sources" / "api.py").write_text(
		f"import sqlload as sl\n\napi = sl.source('api', connection='api', {source})\nhttp = sl.http(api)\n\n"
		f"@api.table(description='Items.')\ndef items(ctx):\n    return {job}\n"
	)
	env = {**os.environ, "HOME": str(tmp_path / "home"), "SSL_CERT_FILE": str(certificates / "ca.pem")}
	return subprocess.run([sys.executable, "-c", "from sqlload._cli import main; main()", "load"], cwd=tmp_path, env=env, capture_output=True, text=True, timeout=120)


def loaded(tmp_path: Path) -> list[tuple[Any, ...]]:
	with duckdb.connect(str(tmp_path / "wh.duckdb"), read_only=True) as connection:
		return connection.execute("SELECT count(*), count(DISTINCT id) FROM raw.api__items").fetchall()


def test_given_one_day_cursor_windows_over_https_when_loading_then_the_windows_share_the_connections_the_first_opened(
	tmp_path: Path, api: TlsServer, certificates: Path
) -> None:
	# Thirteen windows, each three pages of two rows, two of them read ahead together: one pool of connections holds them all.
	source = f"write_strategy='append', cursor_column='updated_at', cursor_start='{START:%Y-%m-%d %H:%M:%S}', batch_size='1d'"
	job = "http.paginate('/items', params={'updated_from': ctx.cursor_value, 'updated_to': ctx.cursor_end}, paginator=sl.offset(limit=2, read_ahead=4), select='$.items')"
	done = load(tmp_path, api, certificates, source, job)
	assert done.returncode == 0, done.stdout + done.stderr
	assert loaded(tmp_path) == [(len(ROWS), len(ROWS))]
	assert api.handshakes <= 4, api.handshakes  # type: ignore[attr-defined]


def test_given_read_ahead_of_twenty_when_a_table_has_few_pages_then_requests_ramp_up_and_stay_close_to_the_pages_read(
	tmp_path: Path, api: TlsServer, certificates: Path
) -> None:
	# Twelve rows over six full pages of two, then an empty page ends the paging: seven pages needed, where twenty at once would send 26.
	job = "http.paginate('/items', paginator=sl.offset(limit=2, read_ahead=20), select='$.items')"
	done = load(tmp_path, api, certificates, "write_strategy='replace'", job.replace("'/items'", "'/items', params={'updated_to': '" + (START + timedelta(days=2, hours=9)).isoformat() + "'}"))
	assert done.returncode == 0, done.stdout + done.stderr
	assert loaded(tmp_path) == [(12, 12)]
	assert api.requests <= 7 + 4, api.requests  # type: ignore[attr-defined]
