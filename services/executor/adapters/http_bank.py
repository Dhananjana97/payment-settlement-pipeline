"""HttpBank (BankGateway port) over httpx, mapped exactly to harness/bank.py.

Facts relied on (bank.py):
- token: 200 {access_token, expires_in}; 429 {error: token_rate_limited} + Retry-After (L156-167)
- payout: 201 body = payout; a replay returns the ORIGINAL status and body with header
  `Idempotent-Replayed: true` (L205) — 201 for a commit, 422 for a cached account_closed
- errors carry {"error": ...}; 429/503 carry Retry-After (L141, L178, L194)
- search: 200 {data: [...], as_of} (L149)

Keep-alive hazard (bank.py:175-180): `503 maintenance` and `401` on POST /v1/payouts are sent
BEFORE the handler reads the request body. The unread JSON stays in the socket and the next request
on that keep-alive connection is parsed from it (observed: 501). Payout POSTs therefore send
`Connection: close`; 20/s new connections is cheap. Token POSTs and GETs are unaffected (the token
handler reads its body first; GETs have none).

Transport failures are split by whether the request can have reached the bank:
- connect errors (ConnectError/ConnectTimeout) → `sent=False`: nothing was transmitted
- anything after the request was written (ReadTimeout, ReadError, RemoteProtocolError, WriteError…)
  → `status=0, sent=True`: outcome unknown
"""
from __future__ import annotations

import httpx

from services.executor.ports import BankPayout, BankResponse

_NOT_SENT = (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout, httpx.UnsupportedProtocol)


def _retry_after(r: httpx.Response) -> float | None:
    v = r.headers.get("Retry-After")
    if v is None:
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _error(r: httpx.Response) -> str | None:
    try:
        body = r.json()
    except ValueError:
        return None
    return body.get("error") if isinstance(body, dict) else None


class HttpBank:
    def __init__(self, base_url: str, client_id: str, client_secret: str, timeout_s: float = 5.0,
                 pool_size: int = 32, connect_timeout_s: float = 2.0):
        self._client_id, self._client_secret = client_id, client_secret
        self._timeout_s = timeout_s
        self._connect_timeout_s = connect_timeout_s
        self._http = httpx.Client(
            base_url=base_url.rstrip("/"),
            limits=httpx.Limits(max_connections=pool_size, max_keepalive_connections=pool_size),
            timeout=httpx.Timeout(timeout_s, connect=connect_timeout_s),
        )

    def close(self) -> None:
        self._http.close()

    def _timeout(self, timeout_s: float | None) -> httpx.Timeout:
        t = self._timeout_s if timeout_s is None else timeout_s
        return httpx.Timeout(t, connect=min(self._connect_timeout_s, t))

    # -- token --------------------------------------------------------------------------
    def mint_token(self) -> tuple[str, int] | float:
        """(token, expires_in_s), or a retry-after delay in seconds on 429 / failure."""
        try:
            r = self._http.post("/v1/oauth/token",
                                json={"client_id": self._client_id, "client_secret": self._client_secret},
                                timeout=self._timeout(None))
        except httpx.HTTPError:
            return 1.0
        if r.status_code == 200:
            body = r.json()
            return str(body["access_token"]), int(body["expires_in"])
        if r.status_code == 429:
            return _retry_after(r) or 1.0
        return 1.0

    # -- payouts ------------------------------------------------------------------------
    def create_payout(self, token: str, key: str, reference: str, account: str, amount_minor: int,
                      currency: str, timeout_s: float) -> BankResponse:
        try:
            r = self._http.post(
                "/v1/payouts",
                json={"reference": reference, "account": account, "amount_minor": int(amount_minor),
                      "currency": currency},
                headers={"Authorization": f"Bearer {token}", "Idempotency-Key": key, "Connection": "close"},
                timeout=self._timeout(timeout_s),
            )
        except _NOT_SENT:
            return BankResponse(status=0, sent=False, error="conn_error")
        except httpx.HTTPError as e:
            return BankResponse(status=0, sent=True, error=f"no_response:{type(e).__name__}")
        replayed = r.headers.get("Idempotent-Replayed", "").lower() == "true"
        if r.status_code in (200, 201):
            try:
                pid = r.json().get("payout_id")
            except ValueError:
                pid = None
            return BankResponse(status=r.status_code, sent=True, payout_id=pid, replayed=replayed)
        return BankResponse(status=r.status_code, sent=True, error=_error(r), replayed=replayed,
                            retry_after_s=_retry_after(r))

    def search(self, token: str, reference: str, timeout_s: float) -> list[BankPayout] | BankResponse:
        try:
            r = self._http.get("/v1/payouts", params={"reference": reference},
                               headers={"Authorization": f"Bearer {token}"}, timeout=self._timeout(timeout_s))
        except _NOT_SENT:
            return BankResponse(status=0, sent=False, error="conn_error")
        except httpx.HTTPError as e:
            return BankResponse(status=0, sent=True, error=f"no_response:{type(e).__name__}")
        if r.status_code != 200:
            return BankResponse(status=r.status_code, sent=True, error=_error(r), retry_after_s=_retry_after(r))
        rows = r.json().get("data", [])
        return [BankPayout(payout_id=d["payout_id"], reference=d["reference"], account=d["account"],
                           amount_minor=int(d["amount_minor"]), currency=d["currency"], status=d["status"],
                           committed_at=float(d["committed_at"])) for d in rows]
