# AI Usage Disclosure

## Tools used
Claude Code (Anthropic's coding agent, Claude Opus model) in my terminal.

## What I used them for
- Reading the brief and the mock provider's source with me, to list every way the provider can
  fail us (503, two kinds of 504, duplicate, lost and early webhooks, the `grace` tolerance, and
  when it frees a tenant slot).
- Writing most of the code from the design below: the SQLite schema and transitions, the asyncio
  dispatcher, the webhook receiver, the CLI, and the chaos harness.
- Writing the first drafts of the tests and of this document set.
- Running the end-to-end chaos runs and measuring what happened (webhook latency, results by
  source).

## What I wrote or decided myself
- Choosing Assignment B over A, and a database-backed state machine over Temporal.
- The core safety argument: write the intent before the request, and settle unknown outcomes by
  looking up the `request_id` rather than re-sending (a re-send is a new dial decision and can
  break the window or phone rules).
- Keeping a slot and phone lock held while an outcome is unknown, and relying on "our count of
  active calls is never lower than the provider's" for R2/R3/R8.
- Enforcing the rules in the schema as well (unique index, trigger), not only in the query.
- Binding a database to a single provider run.

## Where the AI was wrong or unhelpful
- **A race that could double-dial.** In the first version a just-claimed call was only marked
  "in flight" once its HTTP task started. The reconcile loop could run in that gap, see an
  `intent` with no task, look it up (404, because it hadn't been sent yet) and release the lead.
  The original task would then dial anyway. We caught it reviewing the reconcile loop. The fix
  marks the call in-flight in the same step that claims it, and
  `test_claimed_intent_is_not_mistaken_for_an_orphan` pins it.
- **Callbacks losing their phone to another tenant.** The first callback-protection logic only
  held *tenant slots*. A test failed because a fresh call from a different tenant took the phone
  of a lead whose callback was 3 minutes away. Its own tenant had a free slot, but R3 would have
  blocked the callback for the length of that fresh call. Fixed in `reserve()`, and the test now
  asserts both halves.
- **Assuming webhooks are the fast path.** The design treated polling as a rare fallback. When we
  measured a real run, webhooks reached us 0.3–2.5 real seconds after a call ended, while polling
  picked up most results first. That's fine for correctness: results are idempotent. But it
  changed how I describe the design, and it is why the window guard now scales with clock speed:
  a fixed 60 sim-second guard is only 0.1 real seconds at 600×.
- **Checking the window too early, and fsync on every commit.** The first version checked R1
  only when a lead was claimed, and ran SQLite with `synchronous=FULL`. Every chaos run still
  had 0 violations. But when I asked for the actual lag between "decided to dial" and "provider
  accepted", the worst case was ~2000 sim seconds: easily enough to start a call after 19:00 on
  an unlucky run. Fixes: re-check at send, `synchronous=NORMAL` (still kill -9 safe), and no DB
  writes for poll bookkeeping. A test now covers it. Even then, the provider's own accept lag
  reached 925 sim s under CPU contention, more than any fixed guard I'd picked, so the guard now
  adapts to observed lag. Lesson: "0 violations in N runs" was not proof; measuring the margin was.

## How I verified AI-generated code
- Ran the test suite on Python 3.11 as well as 3.12, from a fresh virtualenv.
- Read every module myself, especially `store.py` (each transaction) and the reconcile path.
- 52 unit tests with a fake clock and a scripted fake provider, covering each failure mode and
  each rule.
- End-to-end runs against the unmodified mock with failure injection on, hard-killing the
  dispatcher several times per run (`solution/scripts/chaos_run.py`). Each run passed only if the
  provider's `/admin/report` showed `violation_count: 0` and `final_state.csv` matched
  `/admin/leads` exactly.
