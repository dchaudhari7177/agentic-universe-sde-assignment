"""The lead/attempt state machine and the database-level safety constraints."""
import sqlite3
from datetime import timedelta

import pytest

from solution.store import Store, StateMismatch

from .conftest import SHARED_PHONE, THU_1730, result

ALL = {"c_renew", "c_feedback", "c_zen"}


def reserve(store, tenant, now=THU_1730, campaigns=ALL, **kw):
    return store.reserve(tenant, now, campaigns, "w-test", **kw)


def accept(store, a, now=THU_1730):
    store.mark_accepted(a["request_id"], f"CA{a['request_id'][-12:]}", now, "post")


def finish(store, a, now=THU_1730, **kw):
    return store.apply_result(result(a, ended=now, **kw), now, "webhook")


# ------------------------------------------------------------------ selection & limits
def test_fresh_leads_interleave_across_campaigns(store):
    a1, a2 = reserve(store, "acme"), reserve(store, "acme")
    assert {(a1["campaign_id"], a1["lead_id"]), (a2["campaign_id"], a2["lead_id"])} == {
        ("c_feedback", "f1"), ("c_renew", "r1")}


def test_tenant_limit_is_shared_across_campaigns(store):
    assert reserve(store, "acme") and reserve(store, "acme")
    assert reserve(store, "acme") is None  # max 2 for acme, regardless of campaign
    assert store.live_count("acme") == 2


def test_one_live_call_per_phone_across_tenants(store):
    a = reserve(store, "acme", campaigns={"c_renew"})
    assert a["phone"] == SHARED_PHONE
    z = reserve(store, "zen")
    assert z["lead_id"] == "z2"  # z1 shares the phone that is being dialled by acme


def test_database_rejects_second_live_call_to_same_phone(store):
    reserve(store, "acme", campaigns={"c_renew"})
    with pytest.raises(sqlite3.IntegrityError):
        with store.tx() as c:
            c.execute("INSERT INTO attempts(request_id, campaign_id, lead_id, tenant_id, phone, try_no, state, owner,"
                      " created_sim, created_real) VALUES ('x','c_zen','z1','zen',?,1,'intent','w',0,0)",
                      (SHARED_PHONE,))


def test_database_trigger_enforces_tenant_limit(store):
    reserve(store, "zen")
    with pytest.raises(sqlite3.IntegrityError, match="R2"):
        with store.tx() as c:
            c.execute("INSERT INTO attempts(request_id, campaign_id, lead_id, tenant_id, phone, try_no, state, owner,"
                      " created_sim, created_real) VALUES ('x','c_zen','z2','zen','+919000000022',1,'intent','w',0,0)")


def test_nothing_reserved_when_no_campaign_is_dialable(store):
    assert reserve(store, "acme", campaigns=set()) is None


# ------------------------------------------------------------------ transitions
def test_accept_counts_an_attempt(store):
    a = reserve(store, "zen")
    accept(store, a)
    lead = store.lead("c_zen", a["lead_id"])
    assert (lead["state"], lead["attempts"]) == ("in_call", 1)


def test_503_does_not_count_and_releases_everything(store):
    a = reserve(store, "zen")
    store.mark_not_placed(a["request_id"], THU_1730, THU_1730 + timedelta(seconds=30), "http_503")
    lead = store.lead("c_zen", a["lead_id"])
    assert (lead["state"], lead["attempts"], lead["tries"]) == ("ready", 0, 1)
    assert store.live_count("zen") == 0
    assert reserve(store, "zen")["lead_id"] == "z2"  # z1 now waits 30 s, z2 is next


def test_no_answer_waits_60_minutes_failed_waits_15(store, clock):
    a = reserve(store, "zen")
    accept(store, a)
    finish(store, a, status="no_answer")
    assert store.lead("c_zen", a["lead_id"])["eligible_at"] == (THU_1730 + timedelta(minutes=60)).timestamp()
    b = reserve(store, "zen")
    accept(store, b)
    finish(store, b, status="failed")
    lead = store.lead("c_zen", b["lead_id"])
    assert (lead["queue"], lead["eligible_at"]) == ("retry", (THU_1730 + timedelta(minutes=15)).timestamp())


def test_callback_is_scheduled_at_callback_at(store):
    a = reserve(store, "zen")
    accept(store, a)
    cb = THU_1730 + timedelta(hours=2)
    finish(store, a, disposition="callback_requested", callback_at=cb)
    lead = store.lead("c_zen", a["lead_id"])
    assert (lead["state"], lead["queue"], lead["eligible_at"]) == ("ready", "callback", cb.timestamp())


def test_priority_callbacks_then_retries_then_fresh(store):
    later = THU_1730 + timedelta(hours=3)
    ren = reserve(store, "acme", campaigns={"c_renew"})          # r1 -> no_answer -> retry
    fb = reserve(store, "acme", campaigns={"c_feedback"})        # f1 -> callback
    for a in (ren, fb):
        accept(store, a)
    finish(store, ren, status="no_answer")
    finish(store, fb, disposition="callback_requested", callback_at=THU_1730 + timedelta(minutes=90))
    first = [reserve(store, "acme", now=later) for _ in range(2)]
    assert [x["lead_id"] for x in first] == ["f1", "r1"]  # callback before retry
    store.mark_not_placed(first[0]["request_id"], later, later, "test")
    store.mark_not_placed(first[1]["request_id"], later, later, "test")
    # with only fresh leads competing, the retry still wins over fresh r2/f2
    assert reserve(store, "acme", now=later, campaigns={"c_renew"})["lead_id"] == "r1"


def test_terminal_disposition_completes_lead(store):
    a = reserve(store, "zen")
    accept(store, a)
    finish(store, a, disposition="not_interested")
    lead = store.lead("c_zen", a["lead_id"])
    assert (lead["state"], lead["last_status"], lead["last_disposition"]) == ("completed", "completed", "not_interested")


def test_three_attempts_then_exhausted(store, clock):
    now = THU_1730
    for i in range(3):
        a = reserve(store, "zen", now=now, campaigns={"c_zen"})
        assert a["lead_id"] == "z1"
        accept(store, a, now)
        finish(store, a, now=now, status="busy")
        now += timedelta(minutes=61)
        # keep z2 out of the way
        z2 = store.lead("c_zen", "z2")
        if z2["state"] == "ready":
            store.conn.execute("UPDATE leads SET eligible_at=1e12 WHERE lead_id='z2'")
    lead = store.lead("c_zen", "z1")
    assert (lead["state"], lead["attempts"]) == ("exhausted", 3)
    assert reserve(store, "zen", now=now, campaigns={"c_zen"}) is None


def test_callback_on_last_attempt_is_exhausted(store):
    store.conn.execute("UPDATE leads SET attempts=2 WHERE lead_id='z2'")
    store.conn.execute("UPDATE leads SET eligible_at=1e12 WHERE lead_id='z1'")
    a = reserve(store, "zen")
    accept(store, a)
    finish(store, a, disposition="callback_requested", callback_at=THU_1730 + timedelta(hours=1))
    assert store.lead("c_zen", "z2")["state"] == "exhausted"


def test_dnd_blocks_the_phone_in_every_campaign(store):
    z = reserve(store, "zen", campaigns={"c_zen"})
    assert z["phone"] == SHARED_PHONE
    accept(store, z)
    finish(store, z, disposition="dnd")
    assert store.lead("c_zen", "z1")["state"] == "completed"
    assert store.lead("c_renew", "r1")["state"] == "dnd_blocked"
    a = reserve(store, "acme", campaigns={"c_renew"})
    assert a["lead_id"] != "r1"


def test_exhausted_beats_dnd_blocked(store):
    """Precedence: a lead that used all attempts is exhausted even if its phone later opts out."""
    store.conn.execute("UPDATE leads SET attempts=3, state='exhausted' WHERE lead_id='r1'")
    z = reserve(store, "zen", campaigns={"c_zen"})
    accept(store, z)
    finish(store, z, disposition="dnd")
    assert store.lead("c_renew", "r1")["state"] == "exhausted"


# ------------------------------------------------------------------ idempotency & ordering
def test_duplicate_webhook_is_harmless(store):
    a = reserve(store, "zen")
    accept(store, a)
    assert finish(store, a, status="no_answer") == "applied"
    before = dict(store.lead("c_zen", a["lead_id"]))
    assert finish(store, a, status="no_answer") == "duplicate"
    assert dict(store.lead("c_zen", a["lead_id"])) == before


def test_webhook_before_201(store):
    a = reserve(store, "zen")
    finish(store, a, status="busy")                    # result arrives first
    assert store.mark_accepted(a["request_id"], "CAlate", THU_1730, "post") == "already_ended"
    lead = store.lead("c_zen", a["lead_id"])
    assert (lead["state"], lead["attempts"], lead["queue"]) == ("ready", 1, "retry")
    assert store.live_count("zen") == 0


def test_unknown_result_is_ignored(store):
    assert store.apply_result({"request_id": "nope", "call_sid": "CAx", "status": "busy"}, THU_1730, "webhook") == "unknown"


def test_seeding_is_idempotent_and_bound_to_fixtures(cfg, tmp_path):
    path = str(tmp_path / "again.db")
    Store(path, cfg)
    s2 = Store(path, cfg)
    assert s2.conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0] == 7
    from dataclasses import replace
    with pytest.raises(StateMismatch):
        Store(path, replace(cfg, fingerprint="different"))


def test_database_is_bound_to_one_provider_run(store):
    store.bind_provider("epoch-1")
    store.bind_provider("epoch-1")
    with pytest.raises(StateMismatch):
        store.bind_provider("epoch-2")


def test_journal_is_append_only(store):
    with pytest.raises(sqlite3.IntegrityError):
        store.conn.execute("DELETE FROM journal")
