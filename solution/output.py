"""final_state.csv writer and the comparison against the provider's view."""
from __future__ import annotations

import csv
import os
import tempfile

FIELDS = ["campaign_id", "lead_id", "final_state", "attempts", "last_status", "last_disposition"]


def write_final_state(rows: list[dict], path: str) -> None:
    """Write atomically, so a crash never leaves a half-written file behind."""
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".final_state.", dir=directory)
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS, lineterminator="\n")
            w.writeheader()
            for r in sorted(rows, key=lambda r: (r["campaign_id"], r["lead_id"])):
                w.writerow({k: r[k] for k in FIELDS})
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def read_final_state(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def compare_with_provider(ours: list[dict], provider_leads: list[dict]) -> list[str]:
    """Human-readable differences between final_state.csv and GET /admin/leads."""
    theirs = {(p["campaign_id"], p["lead_id"]): p for p in provider_leads}
    mine = {(r["campaign_id"], r["lead_id"]): r for r in ours}
    diffs = []
    for key in sorted(set(theirs) | set(mine)):
        p, r = theirs.get(key), mine.get(key)
        if p is None or r is None:
            diffs.append(f"{key}: only in {'ours' if p is None else 'provider'}")
            continue
        expected = {"final_state": p["expected_state"], "attempts": str(p["attempts"]),
                    "last_status": p["last_status"] or "", "last_disposition": p["last_disposition"] or ""}
        for k, v in expected.items():
            if str(r[k]) != v:
                diffs.append(f"{key[0]}/{key[1]} {k}: ours={r[k]!r} provider={v!r}")
    return diffs
