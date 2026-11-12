"""Command line entry point.

    python -m solution run       # dispatch until every lead is final, then write final_state.csv
    python -m solution verify    # diff final_state.csv against the provider, print /admin/report
    python -m solution export    # rewrite final_state.csv from the database (no provider needed)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

import uvicorn

from .clock import ProviderClock
from .config import DEFAULT_FIXTURES, load_config
from .dispatcher import Dispatcher, Settings
from .log import log
from .output import compare_with_provider, read_final_state, write_final_state
from .provider import Provider, ProviderUnavailable
from .store import StateMismatch, Store
from .webhooks import create_app

DEFAULT_DB = os.path.join(".state", "dispatcher.db")


def _prepare_state_dir(db_path: str) -> None:
    d = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(d, exist_ok=True)
    marker = os.path.join(d, ".gitignore")
    if os.path.basename(d) == ".state" and not os.path.exists(marker):
        with open(marker, "w") as f:
            f.write("*\n")  # never commit runtime state


async def _wait_for_clock(provider: Provider, timeout_s: float = 60.0) -> dict:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while True:
        try:
            return await provider.clock()
        except ProviderUnavailable as e:
            if loop.time() > deadline:
                raise
            log("waiting_for_provider", error=str(e))
            await asyncio.sleep(1.0)


async def cmd_run(args) -> int:
    cfg = load_config(args.fixtures)
    _prepare_state_dir(args.db)
    store = Store(args.db, cfg)
    provider = Provider(args.provider)
    try:
        clock_payload = await _wait_for_clock(provider)
        store.bind_provider(f"{clock_payload['real_epoch']}|{clock_payload['sim_epoch']}")
        clock = ProviderClock.from_payload(clock_payload)

        settings = Settings(
            callback_url=f"http://{args.public_host or args.host}:{args.port}/webhooks/telephony",
            window_guard_s=args.window_guard,
            latency_budget_real_s=args.latency_budget,
            callback_lookahead_s=args.callback_lookahead,
            poll_after_s=args.poll_after,
        )
        dispatcher = Dispatcher(store, cfg, provider, clock, settings)
        app = create_app(store, clock, cfg.webhook_secret, on_result=dispatcher.wake.set)
        server = uvicorn.Server(uvicorn.Config(app, host=args.host, port=args.port,
                                               log_level="warning", access_log=False))
        serve = asyncio.create_task(server.serve())
        while not server.started:  # webhooks must be receivable before the first call goes out
            if serve.done():
                serve.result()
                return 2
            await asyncio.sleep(0.02)

        await dispatcher.run()
        write_final_state(store.final_rows(), args.out)
        log("final_state_written", path=os.path.abspath(args.out), summary=store.summary())

        server.should_exit = True
        await serve
        return 0
    finally:
        await provider.close()


async def cmd_verify(args) -> int:
    provider = Provider(args.provider)
    try:
        report = await provider.admin("report")
        expected = await provider.admin("leads")
    finally:
        await provider.close()
    ours = read_final_state(args.out)
    diffs = compare_with_provider(ours, expected)
    brief = {k: report[k] for k in ("sim_now", "calls_total", "calls_active", "calls_by_status", "violation_count",
                                    "violations_by_type", "max_concurrency_seen", "callback_lateness_minutes",
                                    "webhooks")}
    print(json.dumps(brief, indent=2))
    print(f"final_state.csv rows: {len(ours)}  provider leads: {len(expected)}  differences: {len(diffs)}")
    for d in diffs[:50]:
        print("  " + d)
    return 0 if not diffs and report["violation_count"] == 0 else 1


def cmd_export(args) -> int:
    store = Store(args.db, load_config(args.fixtures))
    write_final_state(store.final_rows(), args.out)
    print(json.dumps(store.summary(), indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m solution", description="Durable campaign dispatcher")
    ap.add_argument("--db", default=DEFAULT_DB, help=f"SQLite state file (default {DEFAULT_DB})")
    ap.add_argument("--fixtures", default=str(DEFAULT_FIXTURES), help="path to campaigns.json")
    ap.add_argument("--provider", default="http://127.0.0.1:8099", help="telephony provider base URL")
    ap.add_argument("--out", default="final_state.csv", help="where to write final_state.csv")
    sub = ap.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="dispatch until every lead is final")
    run.add_argument("--host", default="127.0.0.1", help="webhook listen address")
    run.add_argument("--port", type=int, default=8200, help="webhook listen port")
    run.add_argument("--public-host", default=None, help="host the provider should call back (default --host)")
    run.add_argument("--window-guard", type=float, default=60.0,
                     help="sim seconds before a window closes after which no new call starts")
    run.add_argument("--latency-budget", type=float, default=0.2,
                     help="real seconds of request latency to allow for at window close (scaled by speed)")
    run.add_argument("--callback-lookahead", type=float, default=360.0,
                     help="sim seconds ahead to reserve tenant slots for due callbacks (0 disables)")
    run.add_argument("--poll-after", type=float, default=180.0,
                     help="sim seconds without a webhook before polling a call")
    sub.add_parser("verify", help="compare final_state.csv with the provider and print its report")
    sub.add_parser("export", help="rewrite final_state.csv from the database")

    args = ap.parse_args(argv)
    # Windows' default Proactor loop can stop accepting connections for good after a peer resets
    # mid-accept (WinError 64). The selector loop doesn't have that failure mode.
    loop_factory = asyncio.SelectorEventLoop if sys.platform == "win32" else None
    try:
        if args.cmd in ("run", "verify"):
            with asyncio.Runner(loop_factory=loop_factory) as runner:  # Python 3.11+
                return runner.run(cmd_run(args) if args.cmd == "run" else cmd_verify(args))
        return cmd_export(args)
    except StateMismatch as e:
        print(f"error: {e}", file=sys.stderr)
        return 3
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
