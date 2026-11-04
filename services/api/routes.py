"""The read API contract (ASSIGNMENT.md:200-214, A6). Routes depend only on the ReadStore port."""
from __future__ import annotations

import time
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse

from services.api.ports import ReadStore
from services.common import metrics


def parse_as_of(value: str) -> datetime:
    # ISO-8601 with a trailing Z or an explicit offset. Naive values are treated as UTC.
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"as_of must be ISO-8601: {value!r}") from e
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def build_router(store: ReadStore) -> APIRouter:
    r = APIRouter()

    @r.get("/healthz")
    async def healthz():
        return {"status": "ok"}

    @r.get("/readyz")
    async def readyz():
        ok = await store.ready()
        body = {"ready": ok, "postgres": ok}
        return JSONResponse(body, status_code=200 if ok else 503)

    @r.get("/v1/merchants/{merchant_id}/balance")
    async def balance(merchant_id: str, as_of: str | None = Query(default=None)):
        if as_of is None:
            return await store.balance(merchant_id)
        return await store.balance_as_of(merchant_id, parse_as_of(as_of))

    @r.get("/v1/platform/balances")
    async def platform_balances():
        return await store.platform_totals()

    @r.get("/v1/payouts/{payout_id}")
    async def payout(payout_id: str):
        row = await store.payout(payout_id)
        if row is None:
            raise HTTPException(status_code=404, detail="payout not applied yet")
        return row

    @r.get("/metrics")
    async def prom_metrics():
        return Response(metrics.render(), media_type="text/plain; version=0.0.4; charset=utf-8")

    return r


async def timing_middleware(request: Request, call_next):
    start = time.perf_counter()
    response = await call_next(request)
    route = request.scope.get("route")
    path = getattr(route, "path", request.url.path)
    metrics.api_request_seconds.labels(route=path, status=str(response.status_code)).observe(
        time.perf_counter() - start
    )
    return response
