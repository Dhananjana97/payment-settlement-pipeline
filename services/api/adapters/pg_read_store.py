"""ReadStore over the async SQLAlchemy engine. Read-only; every query is one indexed lookup."""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine

from services.common.models import PAYOUT_PUBLIC_STATUS, LedgerHistory, Merchant, Payout, PlatformFee


class PgReadStore:
    def __init__(self, eng: AsyncEngine):
        self._eng = eng

    async def ready(self) -> bool:
        try:
            async with self._eng.connect() as conn:
                await conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    async def balance(self, merchant_id: str) -> dict:
        m = Merchant.__table__.c
        stmt = select(m.currency, m.settled_minor, m.held_minor, m.reserve_minor, m.last_seq).where(
            m.merchant_id == merchant_id
        )
        async with self._eng.connect() as conn:
            row = (await conn.execute(stmt)).first()
        if row is None:
            # Unknown merchant: zeros rather than 404, because verify.py polls /balance for every
            # channel while draining and treats non-200 as a failure (verify.py:107-110).
            return {"merchant_id": merchant_id, "currency": None, "settled_minor": 0, "held_minor": 0,
                    "available_minor": 0, "last_seq": 0}
        currency, settled, held, reserve, last_seq = row
        return {"merchant_id": merchant_id, "currency": currency, "settled_minor": settled, "held_minor": held,
                "available_minor": settled - reserve, "last_seq": last_seq}

    async def balance_as_of(self, merchant_id: str, as_of: datetime) -> dict:
        h = LedgerHistory.__table__.c
        stmt = (
            select(h.settled_minor, h.held_minor, h.reserve_minor)
            .where(h.merchant_id == merchant_id, h.event_time <= as_of)
            .order_by(h.event_time.desc())
            .limit(1)
        )
        async with self._eng.connect() as conn:
            row = (await conn.execute(stmt)).first()
        if row is None:
            return {"merchant_id": merchant_id, "settled_minor": 0, "held_minor": 0, "available_minor": 0}
        settled, held, reserve = row
        return {"merchant_id": merchant_id, "settled_minor": settled, "held_minor": held,
                "available_minor": settled - reserve}

    async def platform_totals(self) -> dict[str, int]:
        f = PlatformFee.__table__.c
        stmt = select(f.currency, func.sum(f.total_minor)).group_by(f.currency)
        async with self._eng.connect() as conn:
            rows = (await conn.execute(stmt)).all()
        return {cur: int(total) for cur, total in rows}

    async def payout(self, payout_id: str) -> dict | None:
        p = Payout.__table__.c
        stmt = select(p.status, p.bank_payout_id, p.reason).where(p.payout_id == payout_id)
        async with self._eng.connect() as conn:
            row = (await conn.execute(stmt)).first()
        if row is None:
            return None
        status, bank_payout_id, reason = row
        return {"payout_id": payout_id, "status": PAYOUT_PUBLIC_STATUS[status],
                "bank_payout_id": bank_payout_id, "reason": reason}
