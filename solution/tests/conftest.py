from __future__ import annotations

import json
from datetime import datetime

import pytest

from solution.clock import FakeClock
from solution.config import load_config
from solution.store import Store
from solution.windows import IST

SHARED_PHONE = "+919000000001"  # appears in two campaigns of different tenants


def ist(y, mo, d, h, mi=0, s=0) -> datetime:
    return datetime(y, mo, d, h, mi, s, tzinfo=IST)


THU_1730 = ist(2026, 11, 12, 17, 30)  # the mock's sim start; 12 Nov 2026 is a Thursday


def write_fixtures(tmp_path, tenants=None, campaigns=None, leads=None):
    tenants = tenants or [{"tenant_id": "acme", "max_concurrent_calls": 2},
                          {"tenant_id": "zen", "max_concurrent_calls": 1}]
    campaigns = campaigns or [
        {"campaign_id": "c_renew", "tenant_id": "acme", "leads_file": "renew.csv",
         "window": {"start": "10:00", "end": "19:00", "days": ["mon", "tue", "wed", "thu", "fri", "sat"]},
         "max_attempts": 3},
        {"campaign_id": "c_feedback", "tenant_id": "acme", "leads_file": "feedback.csv",
         "window": {"start": "11:00", "end": "20:30", "days": ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]},
         "max_attempts": 3},
        {"campaign_id": "c_zen", "tenant_id": "zen", "leads_file": "zen.csv",
         "window": {"start": "09:30", "end": "18:00", "days": ["mon", "tue", "wed", "thu", "fri"]},
         "max_attempts": 3},
    ]
    leads = leads or {
        "renew.csv": [("r1", SHARED_PHONE), ("r2", "+919000000002"), ("r3", "+919000000003")],
        "feedback.csv": [("f1", "+919000000011"), ("f2", "+919000000012")],
        "zen.csv": [("z1", SHARED_PHONE), ("z2", "+919000000022")],
    }
    cfg = {
        "sim_start": "2026-11-12T17:30:00+05:30", "timezone": "Asia/Kolkata",
        "regulatory_window": {"start": "09:00", "end": "21:00"}, "window_grace_seconds": 120,
        "retry_policy": {"no_answer_minutes": 60, "busy_minutes": 60, "failed_minutes": 15},
        "webhook_secret": "whsec_test", "tenants": tenants, "campaigns": campaigns,
    }
    (tmp_path / "campaigns.json").write_text(json.dumps(cfg))
    for name, rows in leads.items():
        (tmp_path / name).write_text("lead_id,phone,customer_name\n" +
                                     "".join(f"{lid},{ph},Test Person\n" for lid, ph in rows))
    return tmp_path / "campaigns.json"


@pytest.fixture
def cfg(tmp_path):
    return load_config(write_fixtures(tmp_path))


@pytest.fixture
def store(cfg, tmp_path):
    s = Store(str(tmp_path / "state.db"), cfg)
    s.heartbeat("w-test")
    return s


@pytest.fixture
def clock():
    return FakeClock(THU_1730)


def result(attempt, status="completed", disposition=None, ended=None, callback_at=None, call_sid=None):
    """A webhook body for `attempt`, shaped like the provider's."""
    ended = ended or THU_1730
    return {"event": "call.completed", "call_sid": call_sid or f"CA{attempt['request_id'][-12:]}",
            "request_id": attempt["request_id"], "campaign_id": attempt["campaign_id"],
            "lead_id": attempt["lead_id"], "phone": attempt["phone"], "status": status,
            "disposition": disposition, "callback_at": callback_at.isoformat() if callback_at else None,
            "started_at": ended.isoformat(), "ended_at": ended.isoformat(), "duration_sec": 60}
