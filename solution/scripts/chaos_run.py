#!/usr/bin/env python3
"""End-to-end chaos harness: mock provider + dispatcher(s) + random hard kills + verification.

    python solution/scripts/chaos_run.py --seed 7 --kills 4
    python solution/scripts/chaos_run.py --seed 3 --kills 0 --failure-scale 0   # clean run
    python solution/scripts/chaos_run.py --seed 5 --kills 3 --workers 2         # two dispatchers, one DB

It starts the original, unmodified mock provider (via mock_launcher.py; see the Windows note there), runs `python -m solution run` against it, kills a
dispatcher with SIGKILL (TerminateProcess on Windows) at random moments, restarts it on the same
database, and finally runs `python -m solution verify`. Exit code 0 means zero violations and a
final_state.csv identical to the provider's /admin/leads.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import subprocess
import sys
import tempfile
import time
import urllib.request

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
MOCK_DIR = os.path.join(REPO, "assignment-b-workflow")


def wait_http(url: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1):
                return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError(f"{url} did not come up")


def sim_now(provider: str) -> str:
    try:
        return json.load(urllib.request.urlopen(f"{provider}/v1/clock", timeout=2))["sim_now"]
    except OSError:
        return "?"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=7, help="mock outcome seed")
    ap.add_argument("--kills", type=int, default=3, help="number of hard kills to inject")
    ap.add_argument("--workers", type=int, default=1, help="dispatcher processes sharing one database")
    ap.add_argument("--speed", type=float, default=600.0)
    ap.add_argument("--failure-scale", type=float, default=1.0)
    ap.add_argument("--kill-min", type=float, default=2.0, help="min real seconds between kills")
    ap.add_argument("--kill-max", type=float, default=30.0, help="max real seconds between kills")
    ap.add_argument("--mock-port", type=int, default=8099)
    ap.add_argument("--port", type=int, default=8200, help="first webhook port (worker i uses port+i)")
    ap.add_argument("--timeout", type=float, default=1800.0)
    ap.add_argument("--workdir", default=None, help="where to keep db, logs and final_state.csv")
    ap.add_argument("--rng", type=int, default=None, help="seed for kill timing")
    args = ap.parse_args()

    rng = random.Random(args.rng)
    work = args.workdir or tempfile.mkdtemp(prefix="au-chaos-")
    os.makedirs(work, exist_ok=True)
    db, out = os.path.join(work, "dispatcher.db"), os.path.join(work, "final_state.csv")
    provider = f"http://127.0.0.1:{args.mock_port}"
    print(f"workdir: {work}", flush=True)

    mock_log = open(os.path.join(work, "mock.log"), "w")
    launcher = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mock_launcher.py")
    mock = subprocess.Popen(
        [sys.executable, launcher, "--seed", str(args.seed), "--speed", str(args.speed),
         "--failure-scale", str(args.failure_scale), "--port", str(args.mock_port)],
        cwd=MOCK_DIR, stdout=mock_log, stderr=subprocess.STDOUT)
    procs: dict[int, subprocess.Popen] = {}
    try:
        wait_http(f"{provider}/v1/clock")
        logs = {i: open(os.path.join(work, f"dispatcher{i}.log"), "a") for i in range(args.workers)}

        def start(i: int) -> subprocess.Popen:
            cmd = [sys.executable, "-m", "solution", "--db", db, "--out", out, "--provider", provider,
                   "run", "--port", str(args.port + i)]
            return subprocess.Popen(cmd, cwd=REPO, stdout=logs[i], stderr=logs[i])

        started = time.time()
        for i in range(args.workers):
            procs[i] = start(i)
        kills_left = args.kills
        next_kill = time.time() + rng.uniform(args.kill_min, args.kill_max)
        exit_codes: dict[int, int] = {}
        while len(exit_codes) < args.workers:
            if time.time() - started > args.timeout:
                print("TIMEOUT", flush=True)
                return 124
            for i, p in procs.items():
                if i not in exit_codes and p.poll() is not None:
                    exit_codes[i] = p.returncode
                    print(f"worker {i}: exited {p.returncode} at {time.time() - started:.0f}s (sim {sim_now(provider)})",
                          flush=True)
            alive = [i for i in procs if i not in exit_codes]
            if kills_left and alive and time.time() >= next_kill:
                victim = rng.choice(alive)
                procs[victim].kill()  # SIGKILL on POSIX, TerminateProcess on Windows: no cleanup runs
                procs[victim].wait()
                kills_left -= 1
                print(f"killed worker {victim} at {time.time() - started:.0f}s (sim {sim_now(provider)}); restarting",
                      flush=True)
                procs[victim] = start(victim)
                next_kill = time.time() + rng.uniform(args.kill_min, args.kill_max)
            time.sleep(0.05)

        if any(code != 0 for code in exit_codes.values()):
            print(f"RESULT: FAIL (exit codes {exit_codes})")
            return 1
        verify = subprocess.run([sys.executable, "-m", "solution", "--out", out, "--provider", provider, "verify"],
                                cwd=REPO, capture_output=True, text=True)
        print(verify.stdout)
        if verify.stderr:
            print(verify.stderr, file=sys.stderr)
        print(f"kills injected: {args.kills - kills_left}  wall time: {time.time() - started:.0f}s")
        print("RESULT:", "PASS" if verify.returncode == 0 else "FAIL")
        return verify.returncode
    finally:
        for p in procs.values():
            if p.poll() is None:
                p.kill()
        mock.kill()
        mock.wait()


if __name__ == "__main__":
    sys.exit(main())
