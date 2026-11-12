#!/usr/bin/env python3
"""Start the *unmodified* mock provider, with one Windows-only workaround.

On Windows, uvicorn serves on asyncio's Proactor event loop. That loop's accept() dies with
`OSError: [WinError 64]` when a client is hard-killed while its connection is still queued, after
which the server silently stops accepting connections. The chaos harness hard-kills clients on
purpose, so on Windows we run the mock on the selector loop instead. Nothing in mock_telephony is
changed, and on Linux/macOS this file is just `python -m mock_telephony`.

    python solution/scripts/mock_launcher.py --seed 7 --port 8099   # same flags as the mock
"""
import asyncio
import os
import sys

MOCK_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                        "assignment-b-workflow")
sys.path.insert(0, MOCK_DIR)

if sys.platform == "win32":
    import uvicorn.loops.asyncio as uvicorn_asyncio

    uvicorn_asyncio.asyncio_loop_factory = lambda use_subprocess=False: asyncio.SelectorEventLoop

from mock_telephony.__main__ import main  # noqa: E402

if __name__ == "__main__":
    main()
