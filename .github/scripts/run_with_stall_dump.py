"""Runs a test command and, when it prints nothing for too long, dumps every thread's stack in its test processes and stops it.

Usage: run_with_stall_dump.py SECONDS COMMAND [ARG ...]

libtest prints a mark when each test finishes (and a note when one passes 60 seconds), so a silence
of SECONDS means no test has finished for that long. The dump is taken with gdb before anything is
killed, so it shows the call each stalled thread is blocked in. Exits with the command's status,
or 124 after a stall.
"""

import os
import signal
import subprocess
import sys
import threading
import time

DUMP_SKIP = {"make", "cargo", "rustc", "sh", "bash", "python3", "timeout"}


def descendants(root: int) -> list[int]:
	"""Every process below root, found by walking /proc."""
	children: dict[int, list[int]] = {}
	for entry in os.listdir("/proc"):
		if entry.isdigit():
			try:
				with open(f"/proc/{entry}/stat") as stat:
					parent = int(stat.read().rsplit(")", 1)[1].split()[1])
			except (OSError, IndexError, ValueError):
				continue
			children.setdefault(parent, []).append(int(entry))
	found: list[int] = []
	pending = [root]
	while pending:
		for child in children.get(pending.pop(), []):
			found.append(child)
			pending.append(child)
	return found


def name_of(pid: int) -> str:
	"""The process's command name, or an empty string once it has gone."""
	try:
		with open(f"/proc/{pid}/comm") as comm:
			return comm.read().strip()
	except OSError:
		return ""


def dump(root: int) -> None:
	"""Prints every thread's backtrace for root and each test process below it."""
	for pid in [root, *descendants(root)]:
		name = name_of(pid)
		if not name or name in DUMP_SKIP:
			continue
		print(f"\n::group::Threads of {name} (pid {pid})", flush=True)
		subprocess.run(["sudo", "-n", "gdb", "-p", str(pid), "-batch", "-nx", "-ex", "set pagination off", "-ex", "thread apply all bt"], check=False, timeout=300)
		print("::endgroup::", flush=True)


def main() -> int:
	stall = float(sys.argv[1])
	process = subprocess.Popen(sys.argv[2:], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
	last_output = [time.monotonic()]

	def relay() -> None:
		while chunk := os.read(process.stdout.fileno(), 65536):
			sys.stdout.buffer.write(chunk)
			sys.stdout.buffer.flush()
			last_output[0] = time.monotonic()

	reader = threading.Thread(target=relay, daemon=True)
	reader.start()
	while process.poll() is None:
		if time.monotonic() - last_output[0] > stall:
			print(f"\n::error::No test finished for {stall:.0f} seconds; dumping the test processes' threads.", flush=True)
			dump(process.pid)
			os.killpg(process.pid, signal.SIGKILL)
			process.wait()
			return 124
		time.sleep(5)
	reader.join(timeout=10)
	return process.returncode


if __name__ == "__main__":
	sys.exit(main())
