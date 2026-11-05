"""Running one loader process and measuring it: wall time, CPU of the whole tree, and peak tree RSS."""

import os
import resource
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import psutil

SAMPLE_SECONDS = 0.05


@dataclass
class Measurement:
	"""One process tree's run."""

	wall_s: float
	user_s: float
	sys_s: float
	peak_rss_mb: float
	returncode: int
	output: str

	@property
	def cpu_s(self) -> float:
		return self.user_s + self.sys_s


def _tree_rss(process: psutil.Process) -> int:
	total = 0
	try:
		members = [process, *process.children(recursive=True)]
	except psutil.Error:
		return 0
	for member in members:
		try:
			total += member.memory_info().rss
		except psutil.Error:
			continue
	return total


def run(argv: list[str], *, cwd: Path, env: dict[str, str], log: Path, timeout_s: float = 3600) -> Measurement:
	"""Run `argv` to completion, sampling the summed RSS of it and its descendants.

	CPU comes from `RUSAGE_CHILDREN`, which covers every descendant that was waited for, so loaders
	that start helper processes (ingestr's wrapper and binary) count in full.
	"""
	before = resource.getrusage(resource.RUSAGE_CHILDREN)
	start = time.perf_counter()
	with open(log, "wb") as output:
		child = subprocess.Popen(argv, cwd=cwd, env=env, stdout=output, stderr=subprocess.STDOUT)
		peak = 0
		done = threading.Event()

		def sample() -> None:
			nonlocal peak
			try:
				process = psutil.Process(child.pid)
			except psutil.Error:
				return
			while not done.is_set():
				peak = max(peak, _tree_rss(process))
				done.wait(SAMPLE_SECONDS)

		sampler = threading.Thread(target=sample, daemon=True)
		sampler.start()
		try:
			returncode = child.wait(timeout=timeout_s)
		except subprocess.TimeoutExpired:
			try:
				for grandchild in psutil.Process(child.pid).children(recursive=True):
					grandchild.kill()
			except psutil.Error:
				pass
			child.kill()
			returncode = child.wait()
		wall = time.perf_counter() - start
		done.set()
		sampler.join()
	after = resource.getrusage(resource.RUSAGE_CHILDREN)
	text = log.read_text(errors="replace")
	return Measurement(
		wall_s=wall,
		user_s=after.ru_utime - before.ru_utime,
		sys_s=after.ru_stime - before.ru_stime,
		peak_rss_mb=peak / (1 << 20),
		returncode=returncode,
		output=text[-4000:],
	)


def clean_env(home: Path, extra: dict[str, str]) -> dict[str, str]:
	"""A minimal environment with HOME and XDG folders inside the run's folder."""
	env = {key: os.environ[key] for key in ("PATH", "LANG", "TERM", "TMPDIR") if key in os.environ}
	home.mkdir(parents=True, exist_ok=True)
	env.update(
		HOME=str(home),
		XDG_CONFIG_HOME=str(home / ".config"),
		XDG_CACHE_HOME=str(home / ".cache"),
		XDG_DATA_HOME=str(home / ".local/share"),
		TZ="UTC",
		PYTHONHASHSEED="0",
	)
	env.update(extra)
	return env
