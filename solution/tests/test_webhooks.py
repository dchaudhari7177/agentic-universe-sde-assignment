import hashlib
import hmac
import json

from fastapi.testclient import TestClient

from solution.webhooks import create_app, valid_signature

from .conftest import THU_1730, result


def sign(body: bytes, secret="whsec_test") -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def test_valid_signature():
    body = b'{"a":1}'
    assert valid_signature("whsec_test", body, sign(body))
    assert not valid_signature("whsec_test", body, sign(body, "other"))
    assert not valid_signature("whsec_test", body + b" ", sign(body))
    assert not valid_signature("whsec_test", body, None)


def test_webhook_applies_once_and_rejects_forgeries(store, cfg, clock):
    a = store.reserve("zen", THU_1730, {"c_zen"}, "w-test")
    store.mark_accepted(a["request_id"], "CA1", THU_1730, "post")
    woke = []
    client = TestClient(create_app(store, clock, cfg.webhook_secret, on_result=lambda: woke.append(1)))
    body = json.dumps(result(a, status="no_answer", call_sid="CA1")).encode()

    r = client.post("/webhooks/telephony", content=body, headers={"X-AU-Signature": "0" * 64})
    assert r.status_code == 401
    assert store.attempt(a["request_id"])["state"] == "accepted"

    r = client.post("/webhooks/telephony", content=body, headers={"X-AU-Signature": sign(body)})
    assert r.status_code == 200 and r.json()["outcome"] == "applied"
    r = client.post("/webhooks/telephony", content=body, headers={"X-AU-Signature": sign(body)})
    assert r.status_code == 200 and r.json()["outcome"] == "duplicate"
    assert store.attempt(a["request_id"])["state"] == "ended"
    assert woke == [1, 1]

    status = client.get("/status").json()
    assert status["live_calls_by_tenant"]["zen"] == 0
