"""Ctrl-C during a Python-source load, through the installed `sqlload` package's console entry.

Usage: python interrupt_python_load.py   (with the Python that has the sqlload wheel installed)

A table function yields five rows, then blocks. Once it is blocked, the load gets a real Ctrl-C:
SIGINT on Linux and macOS, and on Windows a console Ctrl-C event in the load's own console. The
load must end within a few seconds as Ctrl-C ends the native executable (the signal's exit status,
or STATUS_CONTROL_C_EXIT on Windows), print no Python traceback, and commit nothing; the next load
must then write every row once. The command runs on a thread of its own, not Python's main thread,
so this checks Ctrl-C still reaches it.

On Windows, a plain Python process gets the same Ctrl-C first. How it ends without clearing an inherited
"ignore Ctrl-C" flag is printed, and it must end once it clears the flag, so a failure of the load is
the load's, not this check's.
"""

import os
import signal
import subprocess
import sys
import tempfile
import textwrap
import time
from pathlib import Path

PROJECT = """\
python_paths = ["sources"]

[connections.lake]
kind = "filesystem"
root = "lake"

[targets.lake]
connection = "lake"
path = "{table}/"
format = "csv"
"""

MODULE = '''
import time
from pathlib import Path

import sqlload as sl

HERE = Path(__file__).resolve().parent.parent

shop = sl.source("shop")


@shop.table(description="Rows that block on the first load.")
def slow(ctx):
    for n in range(5):
        yield {"id": n, "name": f"row {n}"}
    if not (HERE / "release").exists():
        (HERE / "blocked").write_text("blocked")
        time.sleep(120)

'''

#: On Windows, a child inherits its parent's "ignore Ctrl-C" flag, which a CI runner's processes may
#: carry; a process in an interactive console has it clear, so each interrupted child clears it first.
ACCEPT_CTRL_C = "import ctypes; ctypes.windll.kernel32.SetConsoleCtrlHandler(None, False); " if os.name == "nt" else ""

#: `sqlload load`, run as the wheel's console script runs it.
LOAD = [sys.executable, "-c", "from sqlload._cli import main; main()", "load"]

#: The same load, as a process in an interactive console runs it.
INTERRUPTED_LOAD = [sys.executable, "-c", ACCEPT_CTRL_C + "from sqlload._cli import main; main()", "load"]

#: A Python process doing nothing, with Ctrl-C's default action as `sqlload._cli` sets it.
SLEEPER = "import signal, time; signal.signal(signal.SIGINT, signal.SIG_DFL); time.sleep(120)"

#: How Windows ends a process on Ctrl-C when nothing handles it.
STATUS_CONTROL_C_EXIT = 0xC000013A

#: Attaches to the load's console and sends it a Ctrl-C, ignoring the event itself.
WINDOWS_CTRL_C = """
import ctypes, sys
kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
kernel32.FreeConsole()
if not kernel32.AttachConsole(int(sys.argv[1])):
    sys.exit(f"AttachConsole failed: {ctypes.get_last_error()}")
kernel32.SetConsoleCtrlHandler(None, True)
if not kernel32.GenerateConsoleCtrlEvent(0, 0):
    sys.exit(f"GenerateConsoleCtrlEvent failed: {ctypes.get_last_error()}")
"""


def start(command, cwd=None):
    flags = subprocess.CREATE_NEW_CONSOLE if os.name == "nt" else 0
    return subprocess.Popen(command, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8", errors="replace", creationflags=flags)


def interrupt_sleeper(code):
    """Ctrl-C a Python process running `code`, once it has started; whether it ended as Ctrl-C ends it, and how."""
    process = start([sys.executable, "-c", code])
    time.sleep(3)
    ctrl_c(process)
    try:
        process.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
        return False, "still running 10 s after Ctrl-C"
    return interrupted_exit(process.returncode), f"exit {process.returncode}"


def ctrl_c(process):
    if os.name == "nt":
        subprocess.run([sys.executable, "-c", WINDOWS_CTRL_C, str(process.pid)], check=True)
    else:
        process.send_signal(signal.SIGINT)


def interrupted_exit(returncode):
    if os.name == "nt":
        return returncode in (STATUS_CONTROL_C_EXIT, STATUS_CONTROL_C_EXIT - 2**32)
    return returncode in (-signal.SIGINT, 128 + signal.SIGINT)


def written_rows(project):
    lines = [line for path in sorted((project / "lake").rglob("*.csv")) for line in path.read_text(encoding="utf-8").splitlines()[1:]]
    return sorted(int(line.split(",")[0]) for line in lines)


def check(condition, what, output=""):
    if not condition:
        sys.exit(f"{what}\n{output}")
    print(f"ok: {what}", flush=True)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    with tempfile.TemporaryDirectory() as folder:
        project = Path(folder)
        (project / "sources").mkdir()
        (project / "lake").mkdir()
        (project / "sqlload.toml").write_text(PROJECT, encoding="utf-8")
        (project / "sources" / "shop.py").write_text(textwrap.dedent(MODULE), encoding="utf-8")
        if os.name == "nt":
            ended, how = interrupt_sleeper(SLEEPER)
            print(f"note: a Python process with Ctrl-C's default action, as the runner starts it: {how}", flush=True)
            ended, how = interrupt_sleeper(ACCEPT_CTRL_C + SLEEPER)
            check(ended, f"Ctrl-C reaches a Python process that accepts it, so this check can deliver it ({how})")
        process = start(INTERRUPTED_LOAD, project)
        deadline = time.monotonic() + 120
        while not (project / "blocked").exists():
            if process.poll() is not None or time.monotonic() > deadline:
                process.kill()
                out, err = process.communicate()
                sys.exit(f"the load never reached the blocked table function (exit {process.returncode})\n{out}{err}")
            time.sleep(0.2)
        time.sleep(1)
        ctrl_c(process)
        sent = time.monotonic()
        try:
            out, err = process.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            out, err = process.communicate()
            sys.exit(f"the load was still running 15 s after Ctrl-C\n{out}{err}")
        check(time.monotonic() - sent < 5, f"Ctrl-C ended the load within 5 s ({time.monotonic() - sent:.1f} s)", out + err)
        check(interrupted_exit(process.returncode), f"the load ended as Ctrl-C ends it (exit {process.returncode})", out + err)
        check("Traceback" not in err, "no Python traceback", err)
        check(written_rows(project) == [], "the interrupted load committed nothing", str(written_rows(project)))
        (project / "release").write_text("release")
        done = subprocess.run(LOAD, cwd=project, check=False, capture_output=True, text=True, encoding="utf-8", errors="replace")
        check(done.returncode == 0, "the next load succeeds", done.stdout + done.stderr)
        check(written_rows(project) == [0, 1, 2, 3, 4], "the next load writes every row once", str(written_rows(project)))


if __name__ == "__main__":
    main()
