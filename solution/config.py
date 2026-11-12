"""Loads campaigns.json and the lead CSVs into plain, immutable objects."""
from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from .windows import CallingPolicy, DailyWindow

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_FIXTURES = REPO_ROOT / "assignment-b-workflow" / "fixtures" / "campaigns.json"

# R7: dispositions that end a lead. Everything else (callback_requested) keeps it alive.
TERMINAL_DISPOSITIONS = frozenset({"interested", "not_interested", "wrong_number", "dnd"})


@dataclass(frozen=True)
class Campaign:
    campaign_id: str
    tenant_id: str
    max_attempts: int
    policy: CallingPolicy  # campaign window intersected with the regulatory window


@dataclass(frozen=True)
class Lead:
    campaign_id: str
    lead_id: str
    phone: str
    seq: int  # row order in the CSV; used to interleave fresh leads fairly across campaigns


@dataclass(frozen=True)
class Config:
    tenants: dict[str, int]  # tenant_id -> max_concurrent_calls
    campaigns: dict[str, Campaign]
    leads: tuple[Lead, ...]
    retry_after: dict[str, timedelta]  # call status -> wait before the next attempt (R5)
    webhook_secret: str
    grace_seconds: int
    fingerprint: str  # hash of the fixture contents; a database is bound to one fixture set


def load_config(path: str | Path = DEFAULT_FIXTURES) -> Config:
    path = Path(path)
    raw_bytes = path.read_bytes()
    cfg = json.loads(raw_bytes)
    h = hashlib.sha256(raw_bytes)

    reg = cfg["regulatory_window"]
    regulatory = DailyWindow.parse(reg["start"], reg["end"], None)

    tenants = {t["tenant_id"]: int(t["max_concurrent_calls"]) for t in cfg["tenants"]}
    campaigns: dict[str, Campaign] = {}
    leads: list[Lead] = []
    for c in cfg["campaigns"]:
        if c["tenant_id"] not in tenants:
            raise ValueError(f"campaign {c['campaign_id']} references unknown tenant {c['tenant_id']}")
        w = c["window"]
        policy = CallingPolicy((DailyWindow.parse(w["start"], w["end"], w["days"]), regulatory))
        campaigns[c["campaign_id"]] = Campaign(c["campaign_id"], c["tenant_id"], int(c["max_attempts"]), policy)

        leads_path = path.parent / c["leads_file"]
        data = leads_path.read_bytes()
        h.update(data)
        with open(leads_path, newline="", encoding="utf-8") as f:
            for i, row in enumerate(csv.DictReader(f)):
                leads.append(Lead(c["campaign_id"], row["lead_id"].strip(), row["phone"].strip(), i))

    rp = cfg["retry_policy"]
    retry_after = {
        "no_answer": timedelta(minutes=rp["no_answer_minutes"]),
        "busy": timedelta(minutes=rp["busy_minutes"]),
        "failed": timedelta(minutes=rp["failed_minutes"]),
    }
    return Config(
        tenants=tenants,
        campaigns=campaigns,
        leads=tuple(leads),
        retry_after=retry_after,
        webhook_secret=cfg["webhook_secret"],
        grace_seconds=int(cfg.get("window_grace_seconds", 120)),
        fingerprint=h.hexdigest()[:16],
    )
