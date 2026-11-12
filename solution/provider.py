"""Thin async client for the telephony provider. Translates HTTP into outcomes the dispatcher acts on."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import httpx


class ProviderUnavailable(Exception):
    """Could not get an answer (connection refused, timeout, 5xx). Safe to retry later."""


@dataclass
class PlaceOutcome:
    # accepted   - the provider has the call (201 new, or 200 idempotent replay)
    # not_placed - the provider definitely does not have it (503)
    # unknown    - it may or may not exist (504, timeout, dropped connection): look it up
    # rejected   - 4xx: a rule was broken. The provider records this as a violation.
    kind: Literal["accepted", "not_placed", "unknown", "rejected"]
    status_code: int | None = None
    call: dict | None = None
    error: str | None = None


class Provider:
    def __init__(self, base_url: str = "http://127.0.0.1:8099", timeout_s: float = 10.0):
        # The 504 path on the mock takes ~2 s; the timeout leaves plenty of room so a slow
        # response is not mistaken for an unknown outcome.
        self.client = httpx.AsyncClient(base_url=base_url, timeout=httpx.Timeout(timeout_s, connect=3.0))

    async def close(self) -> None:
        await self.client.aclose()

    async def clock(self) -> dict:
        try:
            r = await self.client.get("/v1/clock")
            r.raise_for_status()
            return r.json()
        except httpx.HTTPError as e:
            raise ProviderUnavailable(str(e)) from e

    async def place(self, request_id: str, campaign_id: str, lead_id: str, phone: str,
                    callback_url: str) -> PlaceOutcome:
        body = {"request_id": request_id, "campaign_id": campaign_id, "lead_id": lead_id,
                "phone": phone, "callback_url": callback_url}
        try:
            r = await self.client.post("/v1/calls", json=body)
        except httpx.HTTPError as e:
            # The request may have reached the provider before the connection failed.
            return PlaceOutcome("unknown", error=f"{type(e).__name__}: {e}")
        if r.status_code in (200, 201):
            return PlaceOutcome("accepted", r.status_code, call=r.json())
        if r.status_code == 503:
            return PlaceOutcome("not_placed", 503, error=_err(r))
        if r.status_code in (409, 422, 429, 400):
            return PlaceOutcome("rejected", r.status_code, error=_err(r))
        return PlaceOutcome("unknown", r.status_code, error=_err(r))  # 504 and any other 5xx

    async def find_by_request(self, request_id: str) -> dict | None:
        """The call created with this request_id, or None if the provider has never seen it."""
        return await self._get("/v1/calls", params={"request_id": request_id})

    async def get_call(self, call_sid: str) -> dict | None:
        return await self._get(f"/v1/calls/{call_sid}")

    async def admin(self, path: str) -> dict | list:
        try:
            r = await self.client.get(f"/admin/{path}")
            r.raise_for_status()
            return r.json()
        except httpx.HTTPError as e:
            raise ProviderUnavailable(str(e)) from e

    async def _get(self, path: str, params: dict | None = None) -> dict | None:
        try:
            r = await self.client.get(path, params=params)
        except httpx.HTTPError as e:
            raise ProviderUnavailable(f"{type(e).__name__}: {e}") from e
        if r.status_code == 404:
            return None
        if r.status_code != 200:
            raise ProviderUnavailable(f"HTTP {r.status_code}: {_err(r)}")
        return r.json()


def _err(r: httpx.Response) -> str:
    try:
        j = r.json()
        d = j.get("detail", j) if isinstance(j, dict) else j
        if isinstance(d, dict):
            return str(d.get("error", d))
        return str(d)
    except ValueError:
        return r.text[:200]
