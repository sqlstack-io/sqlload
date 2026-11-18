"""Running one loader process and measuring it: wall time, CPU of the whole tree, and peak tree RSS."""

import os
import resource
import shutil
import signal
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import psutil

SAMPLE_SECONDS = 0.05
#: How often the run's own folders are measured: walking them costs more than reading RSS.
DISK_SAMPLE_SECONDS = 0.5
#: The free disk below which a run is stopped and recorded as aborted for the environment: a full disk fails whichever loader is running, so it says nothing about that loader.
MIN_FREE_DISK_BYTES = 2_000_000_000


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
	#: The most the run's own folder (its HOME, caches and TMPDIR, where a loader spills) held at once; None when it wasn't measured.
	peak_disk_mb: float | None = None
	#: The harness stopped it because the machine's free disk fell below its floor: the environment's failure, not the loader's.
	aborted: bool = False

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
		if self.aborted:
			return f"free disk fell below {MIN_FREE_DISK_BYTES / 1e9:.0f} GB after {self.wall_s:.1f} s, so the run says nothing about the loader"
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


def _folder_bytes(folder: Path) -> int:
	"""The bytes of every file under `folder`, skipping files removed while it is walked."""
	total = 0
	for root, _, files in os.walk(folder):
		for name in files:
			try:
				total += os.lstat(os.path.join(root, name)).st_size
			except OSError:
				continue
	return total


def _stop(pid: int) -> None:
	"""Kill the process `pid` and every descendant."""
	try:
		process = psutil.Process(pid)
		members = [*process.children(recursive=True), process]
	except psutil.Error:
		return
	for member in members:
		try:
			member.kill()
		except psutil.Error:
			continue


def run(
	argv: list[str],
	*,
	cwd: Path,
	env: dict[str, str],
	log: Path,
	timeout_s: float = 3600,
	disk: Path | None = None,
	min_free_bytes: int = MIN_FREE_DISK_BYTES,
) -> Measurement:
	"""Run `argv` to completion, sampling the summed RSS of it and its descendants, and the size of `disk` (the run's own folder) when given.

	When `disk` is given, the free space of its filesystem is checked as often as its size; once it falls below
	`min_free_bytes`, the process tree is killed and the run is marked aborted, so a full disk never reads as a loader failing.

	CPU comes from `RUSAGE_CHILDREN`, which covers every descendant that was waited for, so loaders
	that start helper processes (ingestr's wrapper and binary) count in full.
	"""
	before = resource.getrusage(resource.RUSAGE_CHILDREN)
	oom_before = _oom_kills()
	start = time.perf_counter()
	with open(log, "wb") as output:
		child = subprocess.Popen(argv, cwd=cwd, env=env, stdout=output, stderr=subprocess.STDOUT)
		peak = 0
		peak_disk = 0
		done = threading.Event()
		aborted = threading.Event()

		def sample() -> None:
			nonlocal peak, peak_disk
			try:
				process = psutil.Process(child.pid)
			except psutil.Error:
				return
			next_disk = 0.0
			while not done.is_set():
				peak = max(peak, _tree_rss(process))
				if disk is not None and time.perf_counter() >= next_disk:
					peak_disk = max(peak_disk, _folder_bytes(disk))
					next_disk = time.perf_counter() + DISK_SAMPLE_SECONDS
					if shutil.disk_usage(disk).free < min_free_bytes:
						aborted.set()
						_stop(child.pid)
						return
				done.wait(SAMPLE_SECONDS)

		sampler = threading.Thread(target=sample, daemon=True)
		sampler.start()
		timed_out = False
		try:
			returncode = child.wait(timeout=timeout_s)
		except subprocess.TimeoutExpired:
			timed_out = True
			_stop(child.pid)
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
		peak_disk_mb=None if disk is None else peak_disk / (1 << 20),
		aborted=aborted.is_set(),
	)


def clean_env(home: Path, extra: dict[str, str]) -> dict[str, str]:
	"""A minimal environment with HOME, XDG folders and TMPDIR inside the run's folder, so what a loader spills is counted and removed with the run."""
	env = {key: os.environ[key] for key in ("PATH", "LANG", "TERM") if key in os.environ}
	(home / "tmp").mkdir(parents=True, exist_ok=True)
	env.update(
		HOME=str(home),
		TMPDIR=str(home / "tmp"),
		XDG_CONFIG_HOME=str(home / ".config"),
		XDG_CACHE_HOME=str(home / ".cache"),
		XDG_DATA_HOME=str(home / ".local/share"),
		TZ="UTC",
		PYTHONHASHSEED="0",
	)
	env.update(extra)
	return env
