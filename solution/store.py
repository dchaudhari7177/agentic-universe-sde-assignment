"""Durable state: SQLite is the single source of truth for every lead and every call attempt.

Nothing that matters lives only in memory. Each state transition is one short `BEGIN IMMEDIATE`
transaction, so a `kill -9` at any instant leaves the database at a transition boundary, never
half-way through one.

Two tables drive everything:

* ``leads``    - one row per (campaign, lead): where it is in its lifecycle and when it may be
                 dialled next.
* ``attempts`` - one row per *decision to dial*, keyed by the ``request_id`` we send to the
                 provider. The row is written **before** the HTTP request leaves, so after a crash
                 we always know which requests might exist at the provider and can ask about them.

Safety rules are enforced twice: by the selection query (so we never try), and by constraints and
triggers in the schema (so a bug, or a second dispatcher process, cannot slip past):

* R3 one call per phone  -> partial UNIQUE index on attempts(phone) for live attempts
* R2 tenant concurrency  -> BEFORE INSERT trigger counting the tenant's live attempts
* R4/R7/R8               -> the same trigger refuses leads that are not 'ready', are out of
                            attempts, or whose phone is on the DND list
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Callable, Iterator

from .config import TERMINAL_DISPOSITIONS, Config

LIVE = ("intent", "accepted")  # attempt states that occupy a tenant slot and lock the phone
FINAL_LEAD_STATES = ("completed", "exhausted", "dnd_blocked")

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tenants (
    tenant_id      TEXT PRIMARY KEY,
    max_concurrent INTEGER NOT NULL CHECK (max_concurrent > 0)
);

CREATE TABLE IF NOT EXISTS leads (
    campaign_id      TEXT    NOT NULL,
    lead_id          TEXT    NOT NULL,
    tenant_id        TEXT    NOT NULL REFERENCES tenants(tenant_id),
    phone            TEXT    NOT NULL,
    seq              INTEGER NOT NULL,
    max_attempts     INTEGER NOT NULL,
    state            TEXT    NOT NULL DEFAULT 'ready'
                     CHECK (state IN ('ready','dialing','in_call','completed','exhausted','dnd_blocked')),
    queue            TEXT    NOT NULL DEFAULT 'fresh' CHECK (queue IN ('fresh','retry','callback')),
    eligible_at      REAL    NOT NULL DEFAULT 0,      -- sim epoch seconds; earliest the rules allow
    attempts         INTEGER NOT NULL DEFAULT 0,      -- calls the provider ACCEPTED (R4)
    tries            INTEGER NOT NULL DEFAULT 0,      -- decisions to dial, accepted or not
    last_status      TEXT,
    last_disposition TEXT,
    last_ended_at    REAL,
    updated_sim      REAL,
    PRIMARY KEY (campaign_id, lead_id),
    CHECK (attempts <= max_attempts)
);
CREATE INDEX IF NOT EXISTS leads_pick  ON leads(tenant_id, state, eligible_at);
CREATE INDEX IF NOT EXISTS leads_phone ON leads(phone);

CREATE TABLE IF NOT EXISTS attempts (
    request_id       TEXT    PRIMARY KEY,             -- idempotency key sent to the provider
    campaign_id      TEXT    NOT NULL,
    lead_id          TEXT    NOT NULL,
    tenant_id        TEXT    NOT NULL,
    phone            TEXT    NOT NULL,
    try_no           INTEGER NOT NULL,
    state            TEXT    NOT NULL CHECK (state IN ('intent','accepted','ended','not_placed')),
    owner            TEXT    NOT NULL,                -- worker that decided to dial
    call_sid         TEXT    UNIQUE,
    created_sim      REAL    NOT NULL,
    created_real     REAL    NOT NULL,
    accepted_sim     REAL,
    status           TEXT,
    disposition      TEXT,
    callback_at      REAL,
    ended_at         REAL,
    duration_sec     INTEGER,
    resolved_by      TEXT,                            -- how we learned it was (not) placed: post | lookup
    result_source    TEXT,                            -- how we learned the outcome: webhook | poll | lookup
    note             TEXT,
    FOREIGN KEY (campaign_id, lead_id) REFERENCES leads(campaign_id, lead_id)
);
CREATE INDEX IF NOT EXISTS attempts_live ON attempts(state, tenant_id);
CREATE INDEX IF NOT EXISTS attempts_lead ON attempts(campaign_id, lead_id);
-- R3, enforced by the database: at most one live attempt per phone, across all campaigns.
CREATE UNIQUE INDEX IF NOT EXISTS one_live_call_per_phone
    ON attempts(phone) WHERE state IN ('intent','accepted');

CREATE TABLE IF NOT EXISTS dnd_phones (
    phone      TEXT PRIMARY KEY,
    at_sim     REAL NOT NULL,
    request_id TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS attempts_guard BEFORE INSERT ON attempts
BEGIN
    SELECT RAISE(ABORT, 'R2: tenant concurrency limit')
     WHERE (SELECT COUNT(*) FROM attempts
             WHERE tenant_id = NEW.tenant_id AND state IN ('intent','accepted'))
           >= (SELECT max_concurrent FROM tenants WHERE tenant_id = NEW.tenant_id);
    SELECT RAISE(ABORT, 'R8: phone is on the DND list')
     WHERE EXISTS (SELECT 1 FROM dnd_phones WHERE phone = NEW.phone);
    SELECT RAISE(ABORT, 'R4/R7: lead is not dialable')
     WHERE NOT EXISTS (SELECT 1 FROM leads
                        WHERE campaign_id = NEW.campaign_id AND lead_id = NEW.lead_id
                          AND state = 'ready' AND attempts < max_attempts);
END;

-- Append-only audit trail of every transition and anomaly.
CREATE TABLE IF NOT EXISTS journal (
    id          INTEGER PRIMARY KEY,
    real_ts     REAL NOT NULL,
    sim_ts      REAL,
    kind        TEXT NOT NULL,
    request_id  TEXT,
    campaign_id TEXT,
    lead_id     TEXT,
    detail      TEXT
);
CREATE TRIGGER IF NOT EXISTS journal_no_update BEFORE UPDATE ON journal
BEGIN SELECT RAISE(ABORT, 'journal is append-only'); END;
CREATE TRIGGER IF NOT EXISTS journal_no_delete BEFORE DELETE ON journal
BEGIN SELECT RAISE(ABORT, 'journal is append-only'); END;

CREATE TABLE IF NOT EXISTS workers (
    worker_id      TEXT PRIMARY KEY,
    pid            INTEGER NOT NULL,
    started_real   REAL NOT NULL,
    heartbeat_real REAL NOT NULL
);
"""


def ts(dt: datetime | None) -> float | None:
    return None if dt is None else dt.timestamp()


def parse_ts(s: str | None) -> float | None:
    return None if not s else datetime.fromisoformat(s).timestamp()


def to_dt(epoch: float) -> datetime:
    return datetime.fromtimestamp(epoch, tz=timezone.utc)


class StateMismatch(RuntimeError):
    """The database belongs to a different fixture set or a different provider run."""


class Store:
    def __init__(self, path: str, cfg: Config):
        self.path = path
        self.cfg = cfg
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.conn = sqlite3.connect(path, isolation_level=None, timeout=30, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        # WAL + NORMAL: a committed transaction survives a process crash (kill -9), because the WAL
        # write has reached the OS. FULL would add an fsync per commit, which only buys protection
        # against power loss and stalls the event loop for milliseconds on every transition.
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)
        self._seed()

    # ------------------------------------------------------------------ plumbing
    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")

    def journal(self, kind: str, sim: float | None = None, request_id: str | None = None,
                campaign_id: str | None = None, lead_id: str | None = None, **detail) -> None:
        self.conn.execute(
            "INSERT INTO journal(real_ts, sim_ts, kind, request_id, campaign_id, lead_id, detail)"
            " VALUES (?,?,?,?,?,?,?)",
            (time.time(), sim, kind, request_id, campaign_id, lead_id,
             json.dumps(detail, sort_keys=True) if detail else None))

    def meta(self, key: str) -> str | None:
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def _seed(self) -> None:
        """Load fixtures once. Re-running is a no-op; a different fixture set is refused."""
        with self.tx() as c:
            fp = self.meta("fixtures")
            if fp is None:
                c.execute("INSERT INTO meta VALUES ('fixtures', ?)", (self.cfg.fingerprint,))
                c.execute("INSERT INTO meta VALUES ('namespace', ?)", (uuid.uuid4().hex[:8],))
                c.executemany("INSERT INTO tenants VALUES (?,?)", self.cfg.tenants.items())
                c.executemany(
                    "INSERT INTO leads(campaign_id, lead_id, tenant_id, phone, seq, max_attempts)"
                    " VALUES (?,?,?,?,?,?)",
                    [(l.campaign_id, l.lead_id, self.cfg.campaigns[l.campaign_id].tenant_id, l.phone,
                      l.seq, self.cfg.campaigns[l.campaign_id].max_attempts) for l in self.cfg.leads])
                self.journal("seeded", leads=len(self.cfg.leads))
            elif fp != self.cfg.fingerprint:
                raise StateMismatch(f"{self.path} was created from different fixtures; use a fresh --db")
        self.namespace = self.meta("namespace")

    def bind_provider(self, provider_epoch: str) -> None:
        """A database belongs to exactly one provider run (the mock keeps its state in memory)."""
        with self.tx() as c:
            current = self.meta("provider_epoch")
            if current is None:
                c.execute("INSERT INTO meta VALUES ('provider_epoch', ?)", (provider_epoch,))
            elif current != provider_epoch:
                raise StateMismatch(
                    "the provider was restarted since this database was created (its clock epoch "
                    "changed), so its call records are gone; start again with a fresh --db")

    # ------------------------------------------------------------------ queries
    def live_count(self, tenant_id: str) -> int:
        return self.conn.execute(
            "SELECT COUNT(*) FROM attempts WHERE tenant_id=? AND state IN ('intent','accepted')",
            (tenant_id,)).fetchone()[0]

    def attempt(self, request_id: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM attempts WHERE request_id=?", (request_id,)).fetchone()

    def lead(self, campaign_id: str, lead_id: str) -> sqlite3.Row:
        return self.conn.execute("SELECT * FROM leads WHERE campaign_id=? AND lead_id=?",
                                 (campaign_id, lead_id)).fetchone()

    def all_settled(self) -> bool:
        row = self.conn.execute(
            "SELECT (SELECT COUNT(*) FROM leads WHERE state NOT IN ('completed','exhausted','dnd_blocked')),"
            "       (SELECT COUNT(*) FROM attempts WHERE state IN ('intent','accepted'))").fetchone()
        return row[0] == 0 and row[1] == 0

    def intents(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT a.*, w.heartbeat_real FROM attempts a LEFT JOIN workers w ON w.worker_id = a.owner"
            " WHERE a.state = 'intent' ORDER BY a.created_real").fetchall()

    def accepted_older_than(self, sim_cutoff: float) -> list[sqlite3.Row]:
        """Accepted calls with no result yet, accepted at or before `sim_cutoff`."""
        return self.conn.execute(
            "SELECT * FROM attempts WHERE state = 'accepted' AND accepted_sim <= ? ORDER BY accepted_sim",
            (sim_cutoff,)).fetchall()

    def heartbeat(self, worker_id: str) -> None:
        now = time.time()
        self.conn.execute(
            "INSERT INTO workers VALUES (?,?,?,?) ON CONFLICT(worker_id) DO UPDATE SET heartbeat_real=excluded.heartbeat_real",
            (worker_id, os.getpid(), now, now))

    # ------------------------------------------------------------------ dispatch decision
    def reserve(self, tenant_id: str, now: datetime, dialable: set[str], owner: str,
                callback_lookahead_s: float = 0.0,
                will_be_dialable: Callable[[str, float], bool] | None = None) -> sqlite3.Row | None:
        """Pick the best eligible lead for `tenant_id` and record the decision to dial it.

        Returns the new attempt row (state 'intent') or None if nothing may be dialled now.
        The whole check-and-claim runs in one write transaction, so two dispatchers cannot both
        claim the last slot or the same phone.
        """
        if not dialable:
            return None
        now_s = now.timestamp()
        marks = ",".join("?" * len(dialable))
        with self.tx() as c:
            live = c.execute(
                "SELECT COUNT(*) FROM attempts WHERE tenant_id=? AND state IN ('intent','accepted')",
                (tenant_id,)).fetchone()[0]
            free = self.cfg.tenants[tenant_id] - live
            if free <= 0:
                return None
            # R6 ordering: due callbacks, then due retries, then fresh leads. Within a class the
            # longest-waiting first; fresh leads interleave across campaigns by CSV row order.
            rows = c.execute(
                f"""
                SELECT l.* FROM leads l
                 WHERE l.tenant_id = ? AND l.state = 'ready' AND l.eligible_at <= ?
                   AND l.campaign_id IN ({marks})
                   AND l.attempts < l.max_attempts
                   AND NOT EXISTS (SELECT 1 FROM attempts a
                                    WHERE a.phone = l.phone AND a.state IN ('intent','accepted'))
                   AND NOT EXISTS (SELECT 1 FROM dnd_phones d WHERE d.phone = l.phone)
                 ORDER BY CASE l.queue WHEN 'callback' THEN 0 WHEN 'retry' THEN 1 ELSE 2 END,
                          l.eligible_at, l.seq, l.campaign_id, l.lead_id
                 LIMIT 50
                """, (tenant_id, now_s, *sorted(dialable))).fetchall()
            cand = None
            for row in rows:
                if row["queue"] == "callback" or callback_lookahead_s <= 0:
                    cand = row
                    break
                horizon = now_s + callback_lookahead_s
                # (a) Don't take a phone whose callback (in another campaign) is due within the
                #     lookahead: our call would still be running when the callback falls due.
                if self._phone_has_imminent_callback(row, now_s, horizon, will_be_dialable):
                    continue
                # (b) Keep tenant slots free for this tenant's callbacks falling due soon, so a
                #     callback is not stuck behind a fresh call that has only just started.
                upcoming = c.execute(
                    """
                    SELECT l.campaign_id, l.eligible_at FROM leads l
                     WHERE l.tenant_id = ? AND l.state = 'ready' AND l.queue = 'callback'
                       AND l.eligible_at > ? AND l.eligible_at <= ?
                       AND NOT EXISTS (SELECT 1 FROM attempts a
                                        WHERE a.phone = l.phone AND a.state IN ('intent','accepted'))
                       AND NOT EXISTS (SELECT 1 FROM dnd_phones d WHERE d.phone = l.phone)
                    """, (tenant_id, now_s, horizon)).fetchall()
                n = sum(1 for r in upcoming
                        if will_be_dialable is None or will_be_dialable(r["campaign_id"], r["eligible_at"]))
                if free - 1 < n:
                    return None  # every remaining candidate is non-callback too, so stop here
                cand = row
                break
            if cand is None:
                return None

            try_no = cand["tries"] + 1
            request_id = f"{self.namespace}-{cand['campaign_id']}-{cand['lead_id']}-{try_no}"
            c.execute(
                "INSERT INTO attempts(request_id, campaign_id, lead_id, tenant_id, phone, try_no, state,"
                " owner, created_sim, created_real) VALUES (?,?,?,?,?,?,'intent',?,?,?)",
                (request_id, cand["campaign_id"], cand["lead_id"], tenant_id, cand["phone"], try_no,
                 owner, now_s, time.time()))
            c.execute("UPDATE leads SET state='dialing', tries=?, updated_sim=? WHERE campaign_id=? AND lead_id=?",
                      (try_no, now_s, cand["campaign_id"], cand["lead_id"]))
            self.journal("dial_intent", now_s, request_id, cand["campaign_id"], cand["lead_id"],
                         queue=cand["queue"], try_no=try_no)
            return c.execute("SELECT * FROM attempts WHERE request_id=?", (request_id,)).fetchone()

    def _phone_has_imminent_callback(self, lead: sqlite3.Row, now_s: float, horizon: float,
                                     will_be_dialable: Callable[[str, float], bool] | None) -> bool:
        rows = self.conn.execute(
            "SELECT campaign_id, eligible_at FROM leads WHERE phone = ? AND state = 'ready' AND queue = 'callback'"
            " AND eligible_at <= ? AND NOT (campaign_id = ? AND lead_id = ?)",
            (lead["phone"], horizon, lead["campaign_id"], lead["lead_id"])).fetchall()
        return any(will_be_dialable is None or will_be_dialable(r["campaign_id"], max(r["eligible_at"], now_s))
                   for r in rows)

    # ------------------------------------------------------------------ transitions
    def mark_accepted(self, request_id: str, call_sid: str, now: datetime, how: str) -> str:
        """The provider has this call (201, 200 replay, or found by request_id lookup)."""
        with self.tx() as c:
            a = c.execute("SELECT * FROM attempts WHERE request_id=?", (request_id,)).fetchone()
            if a is None:
                return "unknown"
            if a["state"] == "intent":
                c.execute("UPDATE attempts SET state='accepted', call_sid=?, accepted_sim=?, resolved_by=?"
                          " WHERE request_id=?", (call_sid, now.timestamp(), how, request_id))
                c.execute("UPDATE leads SET state='in_call', attempts=attempts+1, updated_sim=?"
                          " WHERE campaign_id=? AND lead_id=?", (now.timestamp(), a["campaign_id"], a["lead_id"]))
                self.journal("accepted", now.timestamp(), request_id, a["campaign_id"], a["lead_id"],
                             call_sid=call_sid, how=how)
                return "accepted"
            if a["call_sid"] is None:  # result already arrived (webhook before 201); just record the sid
                c.execute("UPDATE attempts SET call_sid=? WHERE request_id=?", (call_sid, request_id))
            if a["state"] == "not_placed":
                self.journal("anomaly", now.timestamp(), request_id, a["campaign_id"], a["lead_id"],
                             detail="provider has a call we had concluded was not placed", how=how)
            return "already_" + a["state"]

    def mark_not_placed(self, request_id: str, now: datetime, retry_at: datetime, reason: str) -> str:
        """The provider definitely does not have this call (503, or 404 on lookup). Release it."""
        with self.tx() as c:
            a = c.execute("SELECT * FROM attempts WHERE request_id=?", (request_id,)).fetchone()
            if a is None or a["state"] != "intent":
                return "noop"
            c.execute("UPDATE attempts SET state='not_placed', resolved_by=?, note=? WHERE request_id=?",
                      ("lookup" if reason.startswith("lookup") else "post", reason, request_id))
            lead = self.lead(a["campaign_id"], a["lead_id"])
            if c.execute("SELECT 1 FROM dnd_phones WHERE phone=?", (a["phone"],)).fetchone():
                new_state = "dnd_blocked"
            else:
                new_state = "ready"
            c.execute("UPDATE leads SET state=?, eligible_at=MAX(eligible_at, ?), updated_sim=?"
                      " WHERE campaign_id=? AND lead_id=?",
                      (new_state, retry_at.timestamp(), now.timestamp(), a["campaign_id"], a["lead_id"]))
            self.journal("not_placed", now.timestamp(), request_id, a["campaign_id"], a["lead_id"],
                         reason=reason, lead_state=new_state, prev_lead_state=lead["state"])
            return "released"

    def apply_result(self, result: dict, now: datetime, source: str) -> str:
        """Apply a call result (webhook body or the `result` of a GET). Idempotent.

        Returns 'applied', 'duplicate' or 'unknown'.
        """
        with self.tx() as c:
            a = None
            if result.get("request_id"):
                a = c.execute("SELECT * FROM attempts WHERE request_id=?", (result["request_id"],)).fetchone()
            if a is None and result.get("call_sid"):
                a = c.execute("SELECT * FROM attempts WHERE call_sid=?", (result["call_sid"],)).fetchone()
            if a is None:
                self.journal("result_unknown", now.timestamp(), result.get("request_id"),
                             result.get("campaign_id"), result.get("lead_id"), source=source)
                return "unknown"
            if a["state"] == "ended":
                self.journal("result_duplicate", now.timestamp(), a["request_id"], a["campaign_id"],
                             a["lead_id"], source=source)
                return "duplicate"

            prev = a["state"]
            status = result.get("status")
            disposition = result.get("disposition")
            ended_at = parse_ts(result.get("ended_at")) or now.timestamp()
            callback_at = parse_ts(result.get("callback_at"))
            c.execute(
                "UPDATE attempts SET state='ended', call_sid=COALESCE(call_sid, ?), status=?, disposition=?,"
                " callback_at=?, ended_at=?, duration_sec=?, accepted_sim=COALESCE(accepted_sim, ?),"
                " resolved_by=COALESCE(resolved_by, 'result'), result_source=? WHERE request_id=?",
                (result.get("call_sid"), status, disposition, callback_at, ended_at,
                 result.get("duration_sec"), parse_ts(result.get("started_at")), source, a["request_id"]))

            key = (a["campaign_id"], a["lead_id"])
            if prev in ("intent", "not_placed"):  # first time we learn the provider accepted it
                c.execute("UPDATE leads SET attempts=attempts+1 WHERE campaign_id=? AND lead_id=?", key)
            c.execute(
                "UPDATE leads SET last_status=?, last_disposition=?, last_ended_at=?"
                " WHERE campaign_id=? AND lead_id=? AND (last_ended_at IS NULL OR last_ended_at <= ?)",
                (status, disposition, ended_at, *key, ended_at))

            lead = self.lead(*key)
            if prev == "not_placed":
                self.journal("anomaly", now.timestamp(), a["request_id"], *key,
                             detail="result for a call we had concluded was not placed", source=source)
            if lead["state"] in ("dialing", "in_call", "ready") and (prev != "not_placed" or lead["state"] == "ready"):
                self._settle(lead, a["request_id"], status, disposition, ended_at, callback_at, now)
            self.journal("result", now.timestamp(), a["request_id"], *key, source=source, status=status,
                         disposition=disposition, prev_attempt_state=prev)
            return "applied"

    def _settle(self, lead: sqlite3.Row, request_id: str, status: str | None, disposition: str | None,
                ended_at: float, callback_at: float | None, now: datetime) -> None:
        """Decide a lead's next state after one of its calls ended (R5-R9)."""
        c = self.conn
        key = (lead["campaign_id"], lead["lead_id"])
        queue, eligible = lead["queue"], lead["eligible_at"]
        if disposition in TERMINAL_DISPOSITIONS:
            state = "completed"
        elif lead["attempts"] >= lead["max_attempts"]:
            state = "exhausted"
        elif c.execute("SELECT 1 FROM dnd_phones WHERE phone=?", (lead["phone"],)).fetchone():
            state = "dnd_blocked"
        else:
            state = "ready"
            if status == "completed" and disposition == "callback_requested" and callback_at is not None:
                queue, eligible = "callback", callback_at
            else:
                wait = self.cfg.retry_after.get(status or "", self.cfg.retry_after["no_answer"])
                queue, eligible = "retry", ended_at + wait.total_seconds()
                if status not in self.cfg.retry_after:
                    self.journal("anomaly", now.timestamp(), request_id, *key,
                                 detail="unexpected result shape, treating as a retry",
                                 status=status, disposition=disposition)
        c.execute("UPDATE leads SET state=?, queue=?, eligible_at=?, updated_sim=? WHERE campaign_id=? AND lead_id=?",
                  (state, queue, eligible, now.timestamp(), *key))

        if disposition == "dnd":
            # R8: the phone is blocked everywhere. Any other lead with this phone that is still
            # waiting becomes dnd_blocked. None can be mid-call: R3 holds the phone lock.
            c.execute("INSERT OR IGNORE INTO dnd_phones VALUES (?,?,?)", (lead["phone"], ended_at, request_id))
            blocked = c.execute(
                "UPDATE leads SET state='dnd_blocked', updated_sim=? WHERE phone=? AND state='ready'"
                " RETURNING campaign_id, lead_id", (now.timestamp(), lead["phone"])).fetchall()
            for b in blocked:
                self.journal("dnd_blocked", now.timestamp(), request_id, b["campaign_id"], b["lead_id"],
                             because=f"{key[0]}/{key[1]}")

    # ------------------------------------------------------------------ reporting
    def final_rows(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT campaign_id, lead_id, state, attempts, last_status, last_disposition FROM leads"
            " ORDER BY campaign_id, lead_id").fetchall()
        return [{"campaign_id": r["campaign_id"], "lead_id": r["lead_id"], "final_state": r["state"],
                 "attempts": r["attempts"], "last_status": r["last_status"] or "",
                 "last_disposition": r["last_disposition"] or ""} for r in rows]

    def summary(self) -> dict:
        by_campaign: dict[str, dict[str, int]] = {}
        for r in self.conn.execute("SELECT campaign_id, state, COUNT(*) n FROM leads GROUP BY 1, 2"):
            by_campaign.setdefault(r["campaign_id"], {})[r["state"]] = r["n"]
        live = {t: self.live_count(t) for t in self.cfg.tenants}
        attempts = dict(self.conn.execute("SELECT state, COUNT(*) FROM attempts GROUP BY state").fetchall())
        anomalies = self.conn.execute("SELECT COUNT(*) FROM journal WHERE kind='anomaly'").fetchone()[0]
        duplicates = self.conn.execute("SELECT COUNT(*) FROM journal WHERE kind='result_duplicate'").fetchone()[0]
        recovered = dict(self.conn.execute(
            "SELECT result_source, COUNT(*) FROM attempts WHERE state='ended' GROUP BY result_source").fetchall())
        return {"leads_by_campaign": by_campaign, "live_calls_by_tenant": live, "attempts_by_state": attempts,
                "results_by_source": recovered, "duplicate_results_ignored": duplicates, "anomalies": anomalies}
