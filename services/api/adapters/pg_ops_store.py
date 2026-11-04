"""PgOpsStore: the A6 operator questions answered from Postgres alone (STEP4-PLAN §2).

One indexed query per section. A failing section returns {"error": ...} for that section only,
so a broken table never hides the rest of the picture. No gateway admin calls: the pipeline must
not depend on the gateway's status port.
"""
from __future__ import annotations

from datetime import datetime
from typing import Awaitable, Callable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from services.common.models import PAYOUT_STATUSES

BEHIND_LIMIT = 20
THROUGHPUT_WINDOW_S = 60
BANK_WINDOW_S = 300

_SESSION = text("""
    SELECT s.epoch, s.holder, s.acquired_at,
           l.holder AS lease_holder,
           EXTRACT(EPOCH FROM (l.expires_at - now())) AS lease_expires_in_s
      FROM session_lease s
      LEFT JOIN leader_lock l ON l.name = 'reader'
     WHERE s.singleton = 1
""")
_CHANNEL_COUNT = text("SELECT count(*) FROM channel_progress")
_BEHIND = text("""
    SELECT channel, last_seq, EXTRACT(EPOCH FROM (now() - updated_at)) AS age_s
      FROM channel_progress
     ORDER BY updated_at ASC
     LIMIT :limit
""")
_THROUGHPUT = text("""
    SELECT outcome, count(*) FROM processed_events
     WHERE applied_at > now() - make_interval(secs => :window)
     GROUP BY outcome
""")
_PAYOUTS = text("SELECT status, count(*) FROM payouts GROUP BY status")
_BANK = text("""
    SELECT http_status, count(*) FROM payout_attempts
     WHERE sent_at > now() - make_interval(secs => :window)
     GROUP BY http_status
""")


def _iso(ts: datetime | None) -> str | None:
    return ts.isoformat() if ts is not None else None


class PgOpsStore:
    def __init__(self, eng: AsyncEngine):
        self._eng = eng

    async def status(self) -> dict:
        out: dict = {}
        for name, fn in (("session", self._session), ("channels", self._channels),
                         ("consumers", self._consumers), ("payouts", self._payouts), ("bank", self._bank)):
            out[name] = await self._section(fn)
        return out

    @staticmethod
    async def _section(fn: Callable[[], Awaitable[dict]]) -> dict:
        try:
            return await fn()
        except Exception as e:  # one broken section must not hide the others
            return {"error": repr(e)}

    async def _session(self) -> dict:
        async with self._eng.connect() as conn:
            row = (await conn.execute(_SESSION)).first()
        if row is None:
            return {"epoch": 0, "holder": None, "since": None, "lease_holder": None, "lease_expires_in_s": None}
        epoch, holder, acquired_at, lease_holder, expires_in = row
        live = expires_in is not None and float(expires_in) > 0
        return {
            "epoch": int(epoch),
            "holder": holder,
            "since": _iso(acquired_at),
            "lease_holder": lease_holder if live else None,
            "lease_expires_in_s": round(float(expires_in), 1) if live else None,
        }

    async def _channels(self) -> dict:
        async with self._eng.connect() as conn:
            count = (await conn.execute(_CHANNEL_COUNT)).scalar() or 0
            rows = (await conn.execute(_BEHIND, {"limit": BEHIND_LIMIT})).all()
        return {
            "count": int(count),
            "behind": [{"channel": ch, "last_seq": int(last), "age_s": round(float(age), 1)} for ch, last, age in rows],
        }

    async def _consumers(self) -> dict:
        async with self._eng.connect() as conn:
            rows = (await conn.execute(_THROUGHPUT, {"window": float(THROUGHPUT_WINDOW_S)})).all()
        outcomes = {o: int(n) for o, n in rows}
        return {"window_s": THROUGHPUT_WINDOW_S, "processed": sum(outcomes.values()), "outcomes": outcomes}

    async def _payouts(self) -> dict:
        async with self._eng.connect() as conn:
            rows = (await conn.execute(_PAYOUTS)).all()
        by_status = {s: 0 for s in PAYOUT_STATUSES}
        by_status.update({s: int(n) for s, n in rows})
        return {"by_status": by_status}

    async def _bank(self) -> dict:
        async with self._eng.connect() as conn:
            rows = (await conn.execute(_BANK, {"window": float(BANK_WINDOW_S)})).all()
        by_status = {("timeout" if code is None else str(code)): int(n) for code, n in rows}
        return {"window_s": BANK_WINDOW_S, "attempts_by_status": by_status}
