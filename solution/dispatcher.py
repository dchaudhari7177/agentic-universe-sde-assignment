"""The dispatcher: decides who to call and when, places calls, and recovers from every failure.

Three cooperating loops share one event loop and one SQLite database:

* dispatch  - claims eligible leads (one DB transaction each) and fires a task per call
* reconcile - resolves attempts whose outcome is unknown and polls calls whose webhook is late
* heartbeat - proves this worker is alive, so others know whose in-flight requests are orphaned

The core rule: **write the intent, then call the provider.** Every request_id is durable before
the HTTP request leaves, so a crash at any line leaves at most a set of "intent" rows. Their fate
is decided by asking the provider (`GET /v1/calls?request_id=`), never by re-sending blindly:
re-sending later could start a call after the window closed or after the lead was settled.
"""
from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

from .clock import Clock
from .config import Config
from .log import log
from .provider import PlaceOutcome, Provider, ProviderUnavailable
from .store import Store, to_dt


@dataclass
class Settings:
    callback_url: str = "http://127.0.0.1:8200/webhooks/telephony"
    window_guard_s: float = 60.0          # don't start a call in the last N sim seconds of a window...
    latency_budget_real_s: float = 0.2    # ...or in the last (this many real seconds x speed), if longer
    callback_lookahead_s: float = 360.0   # hold a slot for callbacks due within N sim seconds
    poll_after_s: float = 180.0           # sim seconds without a result before we poll the call
    poll_every_real_s: float = 0.25       # then poll it at most this often (real seconds)
    tick_real_s: float = 0.02             # dispatch loop wake-up interval (real seconds)
    reconcile_every_real_s: float = 0.25
    heartbeat_every_real_s: float = 0.5
    owner_stale_real_s: float = 2.0       # a worker silent this long is presumed dead
    unknown_settle_real_s: float = 0.5    # wait before looking up a request whose response we lost
    retry_503_s: float = 30.0             # sim seconds before re-trying a lead after a 503
    rejected_backoff_s: float = 1800.0    # sim seconds after an (unexpected) 4xx rule rejection


class Dispatcher:
    def __init__(self, store: Store, cfg: Config, provider: Provider, clock: Clock,
                 settings: Settings | None = None, worker_id: str | None = None):
        self.store = store
        self.cfg = cfg
        self.provider = provider
        self.clock = clock
        self.s = settings or Settings()
        self.worker_id = worker_id or f"w-{uuid.uuid4().hex[:8]}"
        self.inflight: set[str] = set()      # request_ids with a live task in this process
        self.resolving: set[str] = set()
        self.tasks: set[asyncio.Task] = set()
        self.wake = asyncio.Event()
        # The provider checks the window when our request *arrives*. At 600x, 0.2 real seconds of
        # latency is 2 simulated minutes, so the guard scales with the clock speed.
        self.guard = timedelta(seconds=max(self.s.window_guard_s, clock.sim_seconds(self.s.latency_budget_real_s)))
        self.stats = {"placed": 0, "http_503": 0, "unknown": 0, "rejected": 0, "polled_results": 0,
                      "recovered_intents": 0, "skipped_window_closed": 0}
        self.last_polled: dict[str, float] = {}  # request_id -> real time; losing it on a crash is harmless
        # Sim seconds from "decided to dial" to "request sent", and from "sent" to "provider accepted".
        self.queue_lag: list[float] = []
        self.transit_lag: list[float] = []

    # ------------------------------------------------------------------ scheduling
    def dialable_campaigns(self, now: datetime) -> dict[str, set[str]]:
        out: dict[str, set[str]] = {t: set() for t in self.cfg.tenants}
        for c in self.cfg.campaigns.values():
            if c.policy.can_start(now, self.guard):
                out[c.tenant_id].add(c.campaign_id)
        return out

    def _will_be_dialable(self, campaign_id: str, at: float) -> bool:
        return self.cfg.campaigns[campaign_id].policy.can_start(to_dt(at), self.guard)

    def dispatch_once(self, now: datetime) -> list:
        """Claim as many calls as the rules allow right now. Returns the new attempt rows."""
        claimed = []
        for tenant_id, campaigns in self.dialable_campaigns(now).items():
            while True:
                a = self.store.reserve(tenant_id, now, campaigns, self.worker_id,
                                       self.s.callback_lookahead_s, self._will_be_dialable)
                if a is None:
                    break
                # Mark in-flight synchronously, before the task starts, so the reconcile loop
                # never mistakes a just-claimed intent for an orphan and resolves it as "not placed".
                self.inflight.add(a["request_id"])
                claimed.append(a)
        return claimed

    # ------------------------------------------------------------------ placing calls
    def _spawn(self, coro) -> None:
        t = asyncio.create_task(coro)
        self.tasks.add(t)
        t.add_done_callback(self._task_done)

    def _task_done(self, t: asyncio.Task) -> None:
        self.tasks.discard(t)
        if not t.cancelled() and t.exception() is not None:
            log("task_failed", error=repr(t.exception()))
        self.wake.set()

    async def place(self, a) -> None:
        rid = a["request_id"]
        self.inflight.add(rid)
        try:
            # Re-check R1 at the moment of sending, not only at the moment of deciding: if the event
            # loop was busy, the window may have closed in between. Nothing has been sent, so
            # releasing the claim is safe.
            sent = self.clock.now()
            policy = self.cfg.campaigns[a["campaign_id"]].policy
            if not policy.can_start(sent, self.guard):
                self.stats["skipped_window_closed"] += 1
                self.store.mark_not_placed(rid, sent, sent, "window_closed_before_send")
                return
            self.queue_lag.append(sent.timestamp() - a["created_sim"])
            out = await self.provider.place(rid, a["campaign_id"], a["lead_id"], a["phone"], self.s.callback_url)
            if out.kind == "accepted":
                self.transit_lag.append(_created_at(out.call, sent).timestamp() - sent.timestamp())
            await self._handle_place_outcome(a, out)
        finally:
            self.inflight.discard(rid)
            self.wake.set()

    async def _handle_place_outcome(self, a, out: PlaceOutcome) -> None:
        rid = a["request_id"]
        now = self.clock.now()
        if out.kind == "accepted":
            self.stats["placed"] += 1
            self.store.mark_accepted(rid, out.call["call_sid"], _created_at(out.call, now), "post")
            if out.call.get("state") == "ended" and out.call.get("result"):
                self.store.apply_result(out.call["result"], now, "lookup")
        elif out.kind == "not_placed":
            self.stats["http_503"] += 1
            self.store.mark_not_placed(rid, now, now + timedelta(seconds=self.s.retry_503_s), "http_503")
        elif out.kind == "rejected":
            # Should never happen: every rule is checked before we send. If it does, the provider
            # has logged a violation; back off hard so we don't repeat it, and shout.
            self.stats["rejected"] += 1
            log("provider_rejected", request_id=rid, status=out.status_code, error=out.error)
            self.store.mark_not_placed(rid, now, now + timedelta(seconds=self.s.rejected_backoff_s),
                                       f"rejected_{out.status_code}_{out.error}")
        else:
            self.stats["unknown"] += 1
            log("outcome_unknown", request_id=rid, status=out.status_code, error=out.error)
            if out.status_code is None:  # transport error: let any in-flight handling finish first
                await asyncio.sleep(self.s.unknown_settle_real_s)
            await self.resolve(rid)

    async def resolve(self, rid: str, attempts: int = 5) -> bool:
        """Find out whether the provider has `rid`. Returns True if the intent was settled.

        The slot stays held until we know: assuming "not placed" too early could double-dial.
        """
        if rid in self.resolving:
            return False
        self.resolving.add(rid)
        try:
            for i in range(attempts):
                try:
                    call = await self.provider.find_by_request(rid)
                except ProviderUnavailable as e:
                    log("lookup_failed", request_id=rid, error=str(e), try_no=i + 1)
                    await asyncio.sleep(min(2.0, 0.2 * 2 ** i))
                    continue
                now = self.clock.now()
                if call is None:
                    self.store.mark_not_placed(rid, now, now, "lookup_404")
                else:
                    self.store.mark_accepted(rid, call["call_sid"], _created_at(call, now), "lookup")
                    if call.get("state") == "ended" and call.get("result"):
                        self.store.apply_result(call["result"], now, "lookup")
                return True
            return False  # still unknown; the reconcile loop will try again
        finally:
            self.resolving.discard(rid)
            self.wake.set()

    # ------------------------------------------------------------------ reconciliation
    async def reconcile_once(self) -> None:
        now_real = time.time()
        for a in self.store.intents():
            rid = a["request_id"]
            if rid in self.inflight or rid in self.resolving:
                continue
            mine = a["owner"] == self.worker_id
            owner_dead = a["heartbeat_real"] is None or now_real - a["heartbeat_real"] > self.s.owner_stale_real_s
            if mine or owner_dead:
                # Ours but no task (it crashed), or left behind by a worker that died mid-request.
                self.stats["recovered_intents"] += 1
                log("recover_intent", request_id=rid, owner=a["owner"], mine=mine)
                self._spawn(self.resolve(rid))

        now_sim = self.clock.now().timestamp()
        for a in self.store.accepted_older_than(now_sim - self.s.poll_after_s):
            rid = a["request_id"]
            if now_real - self.last_polled.get(rid, 0.0) < self.s.poll_every_real_s:
                continue
            self.last_polled[rid] = now_real
            self._spawn(self._poll(a))

    async def _poll(self, a) -> None:
        try:
            call = await self.provider.get_call(a["call_sid"])
        except ProviderUnavailable as e:
            log("poll_failed", request_id=a["request_id"], error=str(e))
            return
        if call and call.get("state") == "ended" and call.get("result"):
            if self.store.apply_result(call["result"], self.clock.now(), "poll") == "applied":
                self.stats["polled_results"] += 1
                log("result_via_poll", request_id=a["request_id"], call_sid=a["call_sid"])

    # ------------------------------------------------------------------ main loops
    async def _heartbeat_loop(self) -> None:
        while True:
            self.store.heartbeat(self.worker_id)
            await asyncio.sleep(self.s.heartbeat_every_real_s)

    async def _reconcile_loop(self) -> None:
        while True:
            try:
                await self.reconcile_once()
            except Exception as e:  # never let a transient error stop recovery
                log("reconcile_error", error=repr(e))
            await asyncio.sleep(self.s.reconcile_every_real_s)

    async def run(self) -> None:
        """Run until every lead is final and no call is in flight."""
        self.store.heartbeat(self.worker_id)
        log("dispatcher_start", worker=self.worker_id, sim_now=self.clock.now().isoformat())
        background = [asyncio.create_task(self._heartbeat_loop()), asyncio.create_task(self._reconcile_loop())]
        last_report = 0.0
        try:
            while not self.store.all_settled():
                now = self.clock.now()
                try:
                    for a in self.dispatch_once(now):
                        self._spawn(self.place(a))
                except Exception as e:
                    log("dispatch_error", error=repr(e))
                if time.time() - last_report > 5:
                    last_report = time.time()
                    log("progress", sim_now=now.isoformat(), **self.store.summary(), stats=self.stats,
                        lag_sim_s=self.lag_summary())
                try:
                    await asyncio.wait_for(self.wake.wait(), self.s.tick_real_s)
                except asyncio.TimeoutError:
                    pass
                self.wake.clear()
            if self.tasks:
                await asyncio.gather(*self.tasks, return_exceptions=True)
        finally:
            for t in background:
                t.cancel()
            await asyncio.gather(*background, return_exceptions=True)
        log("dispatcher_done", sim_now=self.clock.now().isoformat(), stats=self.stats, lag_sim_s=self.lag_summary())

    def lag_summary(self) -> dict:
        def pct(xs: list[float]) -> dict:
            if not xs:
                return {}
            xs = sorted(xs)
            return {"n": len(xs), "p50": round(xs[len(xs) // 2], 1), "p99": round(xs[int(len(xs) * 0.99)], 1),
                    "max": round(xs[-1], 1)}
        return {"decide_to_send": pct(self.queue_lag), "send_to_accept": pct(self.transit_lag)}


def _created_at(call: dict, fallback: datetime) -> datetime:
    """When the provider accepted the call, by its own clock. accepted_sim - created_sim is then
    exactly how late our request landed, which is what the window guard has to cover."""
    try:
        return datetime.fromisoformat(call["created_at"])
    except (KeyError, TypeError, ValueError):
        return fallback
