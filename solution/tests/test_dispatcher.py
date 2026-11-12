"""Dispatcher behaviour against a scripted fake provider: ambiguity, crashes and scheduling."""
import asyncio
from datetime import timedelta

import pytest

from solution.dispatcher import Dispatcher, Settings
from solution.provider import PlaceOutcome, ProviderUnavailable
from solution.store import Store

from .conftest import ist, result


class FakeProvider:
    """Records calls; `script` decides each POST's fate: 'ok', '503', '504-placed', '504-lost', 'down'."""

    def __init__(self, script=None):
        self.script = list(script or [])
        self.calls: dict[str, dict] = {}   # request_id -> public call
        self.posts: list[str] = []
        self.lookups: list[str] = []
        self.lookup_down = 0

    async def place(self, request_id, campaign_id, lead_id, phone, callback_url):
        self.posts.append(request_id)
        if request_id in self.calls:
            return PlaceOutcome("accepted", 200, call=self.calls[request_id])
        fate = self.script.pop(0) if self.script else "ok"
        if fate == "503":
            return PlaceOutcome("not_placed", 503)
        if fate == "504-lost":          # never placed
            return PlaceOutcome("unknown", 504)
        if fate == "down":
            return PlaceOutcome("unknown", None, error="ConnectError")
        call = {"call_sid": f"CA{len(self.calls):04d}", "request_id": request_id, "state": "active",
                "campaign_id": campaign_id, "lead_id": lead_id}
        self.calls[request_id] = call
        if fate == "504-placed":        # placed, response lost
            return PlaceOutcome("unknown", 504)
        return PlaceOutcome("accepted", 201, call=call)

    async def find_by_request(self, request_id):
        self.lookups.append(request_id)
        if self.lookup_down:
            self.lookup_down -= 1
            raise ProviderUnavailable("down")
        return self.calls.get(request_id)

    async def get_call(self, call_sid):
        return next((c for c in self.calls.values() if c["call_sid"] == call_sid), None)

    def end(self, request_id, **kw):
        c = self.calls[request_id]
        attempt = {"request_id": request_id, "campaign_id": c["campaign_id"], "lead_id": c["lead_id"], "phone": ""}
        c["state"] = "ended"
        c["result"] = result(attempt, call_sid=c["call_sid"], **kw)
        return c["result"]


FAST = Settings(unknown_settle_real_s=0, poll_after_s=600, poll_every_real_s=0, owner_stale_real_s=2.0,
                callback_lookahead_s=0)


def make(store, cfg, clock, provider, settings=FAST, worker="w-test"):
    return Dispatcher(store, cfg, provider, clock, settings, worker_id=worker)


async def drain(d: Dispatcher):
    while d.tasks:
        await asyncio.gather(*list(d.tasks), return_exceptions=True)


async def dispatch(d: Dispatcher):
    for a in d.dispatch_once(d.clock.now()):
        d._spawn(d.place(a))
    await drain(d)


# ------------------------------------------------------------------ R1 windows
def test_only_open_campaigns_dial(store, cfg, clock):
    d = make(store, cfg, clock, FakeProvider())
    clock.current = ist(2026, 11, 12, 18, 30)   # zen closed (18:00), renew + feedback open
    assert d.dialable_campaigns(clock.now()) == {"acme": {"c_renew", "c_feedback"}, "zen": set()}
    clock.current = ist(2026, 11, 12, 18, 59, 30)  # inside renew's last minute: guard says no
    assert d.dialable_campaigns(clock.now())["acme"] == {"c_feedback"}
    clock.current = ist(2026, 11, 15, 12, 0)    # Sunday: only feedback
    assert d.dialable_campaigns(clock.now()) == {"acme": {"c_feedback"}, "zen": set()}


def test_nothing_dialled_at_night(store, cfg, clock):
    clock.current = ist(2026, 11, 12, 21, 30)
    d = make(store, cfg, clock, FakeProvider())
    assert d.dispatch_once(clock.now()) == []


# ------------------------------------------------------------------ provider failure modes
@pytest.mark.asyncio
async def test_503_releases_slot_without_counting(store, cfg, clock):
    p = FakeProvider(["503", "ok", "ok"])
    d = make(store, cfg, clock, p)
    await dispatch(d)
    assert len(p.calls) == 2
    assert store.conn.execute("SELECT COUNT(*) FROM attempts WHERE state='not_placed'").fetchone()[0] == 1
    assert sum(r["attempts"] for r in store.conn.execute("SELECT attempts FROM leads")) == 2


@pytest.mark.asyncio
async def test_504_placed_is_found_and_never_redialled(store, cfg, clock):
    p = FakeProvider(["504-placed"])
    d = make(store, cfg, clock, p)
    await dispatch(d)
    rid = p.posts[0]
    assert store.attempt(rid)["state"] == "accepted"
    assert store.attempt(rid)["resolved_by"] == "lookup"
    assert p.posts.count(rid) == 1  # resolved by lookup, not by re-sending


@pytest.mark.asyncio
async def test_504_not_placed_is_released(store, cfg, clock):
    p = FakeProvider(["504-lost"])
    d = make(store, cfg, clock, p)
    await dispatch(d)
    rid = p.posts[0]
    a = store.attempt(rid)
    assert a["state"] == "not_placed" and a["note"] == "lookup_404"
    assert store.lead(a["campaign_id"], a["lead_id"])["attempts"] == 0


@pytest.mark.asyncio
async def test_unknown_outcome_holds_slot_while_provider_is_down(store, cfg, clock):
    p = FakeProvider(["down"])
    p.lookup_down = 100
    d = make(store, cfg, clock, p)
    store.conn.execute("UPDATE leads SET eligible_at=1e12 WHERE campaign_id <> 'c_zen' OR lead_id <> 'z2'")
    a = d.dispatch_once(clock.now())[0]
    await d.place(a)
    assert store.attempt(a["request_id"])["state"] == "intent"
    assert store.live_count("zen") == 1  # still occupies the tenant slot: we don't know yet
    assert d.dispatch_once(clock.now()) == []


@pytest.mark.asyncio
async def test_webhook_lost_recovered_by_polling(store, cfg, clock):
    p = FakeProvider()
    d = make(store, cfg, clock, p)
    await dispatch(d)
    rid = p.posts[0]
    p.end(rid, status="no_answer", ended=clock.now())
    await d.reconcile_once(); await drain(d)
    assert store.attempt(rid)["state"] == "accepted"  # too early to poll
    clock.advance(minutes=11)
    await d.reconcile_once(); await drain(d)
    a = store.attempt(rid)
    assert (a["state"], a["result_source"]) == ("ended", "poll")


# ------------------------------------------------------------------ crash recovery
@pytest.mark.asyncio
async def test_crash_after_intent_before_post(store, cfg, clock, tmp_path):
    """Process dies after committing the intent but before the request left: not placed."""
    p = FakeProvider()
    dead = make(store, cfg, clock, p, worker="w-dead")
    store.heartbeat("w-dead")
    a = dead.dispatch_once(clock.now())[0]          # intent committed; process "dies" here
    store.conn.execute("UPDATE workers SET heartbeat_real = heartbeat_real - 60 WHERE worker_id='w-dead'")

    fresh = make(Store(store.path, cfg), cfg, clock, p, worker="w-new")
    await fresh.reconcile_once(); await drain(fresh)
    row = fresh.store.attempt(a["request_id"])
    assert row["state"] == "not_placed"
    assert a["request_id"] not in p.posts           # we asked, we did not blindly re-send


@pytest.mark.asyncio
async def test_crash_after_post_before_response(store, cfg, clock):
    """Process dies after the provider accepted but before we recorded it: found, counted once."""
    p = FakeProvider()
    dead = make(store, cfg, clock, p, worker="w-dead")
    store.heartbeat("w-dead")
    a = dead.dispatch_once(clock.now())[0]
    await p.place(a["request_id"], a["campaign_id"], a["lead_id"], a["phone"], "cb")  # reached provider
    store.conn.execute("UPDATE workers SET heartbeat_real = heartbeat_real - 60 WHERE worker_id='w-dead'")

    fresh = make(Store(store.path, cfg), cfg, clock, p, worker="w-new")
    await fresh.reconcile_once(); await drain(fresh)
    row = fresh.store.attempt(a["request_id"])
    assert row["state"] == "accepted"
    assert fresh.store.lead(a["campaign_id"], a["lead_id"])["attempts"] == 1


@pytest.mark.asyncio
async def test_live_workers_intents_are_left_alone(store, cfg, clock):
    p = FakeProvider()
    other = make(store, cfg, clock, p, worker="w-other")
    store.heartbeat("w-other")                      # alive and well
    a = other.dispatch_once(clock.now())[0]
    me = make(store, cfg, clock, p, worker="w-me")
    await me.reconcile_once(); await drain(me)
    assert store.attempt(a["request_id"])["state"] == "intent"
    assert p.lookups == []


@pytest.mark.asyncio
async def test_claimed_intent_is_not_mistaken_for_an_orphan(store, cfg, clock):
    """Regression: reconcile running between claim and task start must not release the call."""
    p = FakeProvider()
    d = make(store, cfg, clock, p)
    claimed = d.dispatch_once(clock.now())
    await d.reconcile_once()
    assert p.lookups == []
    for a in claimed:
        d._spawn(d.place(a))
    await drain(d)
    assert all(store.attempt(a["request_id"])["state"] == "accepted" for a in claimed)


# ------------------------------------------------------------------ R6 callbacks
def test_slot_is_held_for_an_imminent_callback(store, cfg, clock):
    clock.current = ist(2026, 11, 13, 12, 0)
    settings = Settings(callback_lookahead_s=360)
    d = make(store, cfg, clock, FakeProvider(), settings)
    soon = clock.now() + timedelta(minutes=3)
    store.conn.execute("UPDATE leads SET queue='callback', eligible_at=? WHERE lead_id='z1'", (soon.timestamp(),))
    first = d.dispatch_once(clock.now())
    assert [a for a in first if a["tenant_id"] == "zen"] == []        # z2 waits: zen's only slot is held
    assert "r1" not in {a["lead_id"] for a in first}                  # acme leaves z1's phone alone
    for a in first:
        store.mark_not_placed(a["request_id"], clock.now(), clock.now(), "test")
    clock.current = soon
    got = [a for a in d.dispatch_once(clock.now()) if a["tenant_id"] == "zen"]
    assert [a["lead_id"] for a in got] == ["z1"]


def test_callback_outside_window_is_dialled_at_next_opening(store, cfg, clock):
    """R6: callback_at after close -> first instant of the next window, ahead of everything else."""
    d = make(store, cfg, clock, FakeProvider())
    store.conn.execute("UPDATE leads SET eligible_at=1e12 WHERE campaign_id='c_feedback'")
    cb = ist(2026, 11, 12, 20, 15)                 # renewals closed at 19:00 Thursday
    store.conn.execute("UPDATE leads SET queue='callback', eligible_at=? WHERE lead_id='r3'", (cb.timestamp(),))

    clock.current = ist(2026, 11, 12, 20, 16)
    assert [a for a in d.dispatch_once(clock.now()) if a["tenant_id"] == "acme"] == []
    clock.current = ist(2026, 11, 13, 9, 59, 59)
    assert [a for a in d.dispatch_once(clock.now()) if a["tenant_id"] == "acme"] == []
    clock.current = ist(2026, 11, 13, 10, 0)       # Friday opening: the callback goes first
    got = [a["lead_id"] for a in d.dispatch_once(clock.now()) if a["tenant_id"] == "acme"]
    assert got[0] == "r3"


def test_retry_waits_exactly_until_eligible(store, cfg, clock):
    """R5 end to end: a no_answer at 12:00 is not redialled at 12:59:59, and is at 13:00."""
    clock.current = ist(2026, 11, 13, 12, 0)
    d = make(store, cfg, clock, FakeProvider())
    store.conn.execute("UPDATE leads SET eligible_at=1e12 WHERE lead_id <> 'z2'")
    a = d.dispatch_once(clock.now())[0]
    store.mark_accepted(a["request_id"], "CA1", clock.now(), "post")
    store.apply_result(result(a, status="no_answer", ended=clock.now(), call_sid="CA1"), clock.now(), "webhook")
    clock.current = ist(2026, 11, 13, 12, 59, 59)
    assert d.dispatch_once(clock.now()) == []
    clock.current = ist(2026, 11, 13, 13, 0)
    assert [x["lead_id"] for x in d.dispatch_once(clock.now())] == ["z2"]


@pytest.mark.asyncio
async def test_window_rechecked_at_send_time(store, cfg, clock):
    """R1: claimed just before close, but the loop stalled past it -> never sent, claim released."""
    p = FakeProvider()
    clock.current = ist(2026, 11, 12, 18, 57)      # renew still open (closes 19:00, guard 2 min at 600x)
    d = make(store, cfg, clock, p)
    store.conn.execute("UPDATE leads SET eligible_at=1e12 WHERE campaign_id <> 'c_renew'")
    claimed = d.dispatch_once(clock.now())
    assert claimed
    clock.current = ist(2026, 11, 12, 18, 58, 30)  # event loop was busy for 90 sim seconds
    for a in claimed:
        await d.place(a)
    assert p.posts == []
    assert {store.attempt(a["request_id"])["note"] for a in claimed} == {"window_closed_before_send"}
    assert store.live_count("acme") == 0
