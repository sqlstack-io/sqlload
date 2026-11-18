"""Running one loader process and measuring it: wall time, CPU of the whole tree, and peak tree RSS."""

import os
import resource
import signal
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
	#: The harness stopped it at its time limit.
	timed_out: bool = False
	#: Processes the out-of-memory killer ended in the harness's cgroup during the run; None where that count can't be read.
	oom_kills: int | None = None

	@property
	def cpu_s(self) -> float:
		return self.user_s + self.sys_s

	@property
	def signal(self) -> str | None:
		"""The signal that ended the loader, such as `SIGKILL`, when one did."""
		if self.returncode >= 0:
			return None
		try:
			return signal.Signals(-self.returncode).name
		except ValueError:
			return f"signal {-self.returncode}"

	def failure(self) -> str:
		"""How a run that didn't succeed ended: its exit code or signal, how long it ran, and its peak memory."""
		if self.timed_out:
			how = "timed out"
		elif self.signal:
			how = f"killed by {self.signal}" + (" (out of memory)" if self.oom_kills else "")
		else:
			how = f"exited {self.returncode}"
		ran = f"{self.wall_s:.1f} s" if self.wall_s < 120 else f"{self.wall_s / 60:.1f} min"
		peak = f"{self.peak_rss_mb / 1024:.2f} GiB" if self.peak_rss_mb >= 1024 else f"{self.peak_rss_mb:.0f} MiB"
		return f"{how} after {ran}, at {peak} peak RSS"


def _oom_kills() -> int | None:
	"""How many processes the out-of-memory killer has ended in this process's cgroup (cgroup v2), counting its descendants."""
	try:
		path = Path("/proc/self/cgroup").read_text().splitlines()[0].split("::", 1)[1]
		events = Path(f"/sys/fs/cgroup{path}/memory.events").read_text()
	except (OSError, IndexError):
		return None
	counts = dict(line.split() for line in events.splitlines() if line.count(" ") == 1)
	return int(counts["oom_kill"]) if "oom_kill" in counts else None


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
	oom_before = _oom_kills()
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
		timed_out = False
		try:
			returncode = child.wait(timeout=timeout_s)
		except subprocess.TimeoutExpired:
			timed_out = True
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
	oom_after = _oom_kills()
	text = log.read_text(errors="replace")
	return Measurement(
		wall_s=wall,
		user_s=after.ru_utime - before.ru_utime,
		sys_s=after.ru_stime - before.ru_stime,
		peak_rss_mb=peak / (1 << 20),
		returncode=returncode,
		output=text[-4000:],
		timed_out=timed_out,
		oom_kills=None if oom_before is None or oom_after is None else oom_after - oom_before,
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
