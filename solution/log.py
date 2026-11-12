"""Structured (JSON lines) logging to stderr."""
from __future__ import annotations

import json
import sys
import time


def log(event: str, **fields) -> None:
    rec = {"ts": round(time.time(), 3), "event": event, **fields}
    sys.stderr.write(json.dumps(rec, default=str) + "\n")
    sys.stderr.flush()
