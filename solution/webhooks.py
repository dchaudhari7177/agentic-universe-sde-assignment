"""HTTP surface: the provider's result webhook, plus /status and /healthz for operators."""
from __future__ import annotations

import hashlib
import hmac
import json

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .clock import Clock
from .log import log
from .store import Store


def valid_signature(secret: str, body: bytes, signature: str | None) -> bool:
    if not signature:
        return False
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature.strip().lower())


def create_app(store: Store, clock: Clock, secret: str, on_result=None) -> FastAPI:
    app = FastAPI(title="Agentic Universe dispatcher", docs_url=None, redoc_url=None)

    @app.post("/webhooks/telephony")
    async def telephony_webhook(request: Request):
        body = await request.body()  # verify the exact bytes that were signed
        if not valid_signature(secret, body, request.headers.get("X-AU-Signature")):
            log("webhook_bad_signature", delivery=request.headers.get("X-AU-Delivery-Id"))
            return JSONResponse({"error": "invalid_signature"}, status_code=401)
        try:
            payload = json.loads(body)
        except ValueError:
            return JSONResponse({"error": "invalid_json"}, status_code=400)
        if not isinstance(payload, dict) or payload.get("event") != "call.completed":
            return JSONResponse({"ok": True, "outcome": "ignored"})
        # Commit before replying 2xx: if we die first, the provider redelivers (and polling backs it up).
        outcome = store.apply_result(payload, clock.now(), "webhook")
        if on_result:
            on_result()
        return {"ok": True, "outcome": outcome}

    @app.get("/status")
    async def status():
        return {"sim_now": clock.now().isoformat(), "settled": store.all_settled(), **store.summary()}

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    return app
