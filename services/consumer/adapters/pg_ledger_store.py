"""PgLedgerStore: the per-partition batch transaction (STEP3-PLAN §2.1; D4 amended, D9, D18).

One `engine.begin()` per batch. The batch is applied in memory under row locks and written back
with a handful of set-based statements, so the SQL round trips per batch are O(distinct rows), not
O(events):

  1. decode everything; bulk-insert processed_events (ON CONFLICT DO NOTHING RETURNING) → dedupe set
  2. lock + load channel_progress and merchants for every channel in the batch (FOR UPDATE)
  3. per record, in partition order: stale-epoch → dedupe → contiguity (in memory) → rules.apply →
     effects applied to in-memory row state; one ledger_history row per DATA frame
  4. write back: merchants, channel_progress (compare-and-set), auths, captures, payouts,
     ledger_history, processed_events outcomes (+ deletes for out-of-order), platform_fees shard
Nothing is visible until COMMIT: a crash anywhere replays the batch and every event dedupes.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy import Engine, bindparam, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from services.common.messages import LedgerMessage, MessageError, decode
from services.common.models import (Auth, Capture, ChannelProgress, LedgerHistory, Merchant, Payout,
                                    PlatformFee, ProcessedEvent)
from services.consumer.domain import rules
from services.consumer.domain.effects import (AddPlatformFee, AddRefunded, AdjustBalance, AuthRow, CaptureRow,
                                              CloseAuth, MerchantBook, OpenAuth, RecordPayout, SetConfig,
                                              UpsertCapture)
from services.consumer.ports import Outcome, Record

_M, _A, _C, _PE, _CP, _LH, _PF, _PAY = (Merchant.__table__, Auth.__table__, Capture.__table__, ProcessedEvent.__table__,
                                       ChannelProgress.__table__, LedgerHistory.__table__, PlatformFee.__table__,
                                       Payout.__table__)
_FENCE = text("SELECT epoch FROM session_lease WHERE singleton = 1")


class BatchConflict(RuntimeError):
    """Another writer moved a channel we had locked in memory (should not happen: one partition owner).
    The app retries the batch; every event then dedupes or applies in order."""


def _parse_time(value: str) -> datetime:
    ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


class PgLedgerStore:
    def __init__(self, engine: Engine):
        self._eng = engine

    def fence_epoch(self) -> int:
        with self._eng.connect() as conn:
            return int(conn.execute(_FENCE).scalar() or 0)

    def apply_batch(self, partition: int, records: list[Record], fence_epoch: int) -> list[Outcome]:
        with self._eng.begin() as conn:
            b = _Batch(conn, partition, fence_epoch)
            b.prepare(records)
            outcomes = [b.apply(rec, msg) for rec, msg in b.decoded]
            b.finish()
        return outcomes


@dataclass
class _Progress:
    last_seq: int
    epoch: int
    existed: bool
    orig_last_seq: int
    changed: bool = False


class _Lookup:
    """Lookup port: in-batch state first (rows this batch created/changed), then the DB row FOR UPDATE."""

    def __init__(self, batch: "_Batch"):
        self.b = batch

    def auth(self, event_id: str) -> AuthRow | None:
        a = self.b.auths.get(event_id)
        if a is None and event_id not in self.b.auths:
            row = self.b.conn.execute(
                select(_A.c.event_id, _A.c.amount_minor, _A.c.is_open).where(_A.c.event_id == event_id).with_for_update()
            ).first()
            a = AuthRow(*row) if row else None
            self.b.auths[event_id] = a
        return a

    def capture(self, event_id: str) -> CaptureRow | None:
        c = self.b.captures.get(event_id)
        if c is None and event_id not in self.b.captures:
            row = self.b.conn.execute(
                select(_C.c.event_id, _C.c.amount_minor, _C.c.refunded_minor)
                .where(_C.c.event_id == event_id).with_for_update()
            ).first()
            c = CaptureRow(*row) if row else None
            self.b.captures[event_id] = c
        return c


class _Batch:
    def __init__(self, conn, partition: int, fence_epoch: int):
        self.conn = conn
        self.partition = partition
        self.fence = fence_epoch
        self.decoded: list[tuple[Record, LedgerMessage | None]] = []
        self.inserted: set[str] = set()          # event_ids this batch inserted into processed_events
        self.seen: set[str] = set()              # event_ids already handled in this batch
        self.progress: dict[str, _Progress] = {}
        self.books: dict[str, MerchantBook] = {}
        self.new_merchants: set[str] = set()
        self.dirty_merchants: set[str] = set()
        self.auths: dict[str, AuthRow | None] = {}
        self.touched_auths: set[str] = set()
        self.captures: dict[str, CaptureRow | None] = {}
        self.touched_captures: set[str] = set()
        self.row_merchant: dict[str, str] = {}   # auth/capture event_id -> merchant (from the message channel)
        self.payouts: list[dict] = []
        self.history: list[dict] = []
        self.outcomes: dict[str, str] = {}       # event_id -> outcome to store
        self.drop_pe: list[str] = []             # processed_events rows to delete (out of order)
        self.fees: dict[str, int] = defaultdict(int)
        self.lookup = _Lookup(self)

    # -- 1 + 2: decode, dedupe, lock ------------------------------------------------------
    def prepare(self, records: list[Record]) -> None:
        pe_rows, channels = [], set()
        for rec in records:
            try:
                msg = decode(rec.value)
            except MessageError:
                self.decoded.append((rec, None))
                continue
            self.decoded.append((rec, msg))
            if msg.session_epoch < self.fence:
                continue
            channels.add(msg.channel)
            if msg.kind == "DATA":
                eid = (msg.event or {}).get("event_id")
                if isinstance(eid, str) and eid not in {r["event_id"] for r in pe_rows}:
                    pe_rows.append({"event_id": eid, "channel": msg.channel, "seq": msg.seq, "outcome": "pending"})
        if pe_rows:
            res = self.conn.execute(
                pg_insert(_PE).on_conflict_do_nothing(index_elements=[_PE.c.event_id]).returning(_PE.c.event_id),
                pe_rows,
            )
            self.inserted = {r[0] for r in res.all()}
        if channels:
            chans = sorted(channels)
            for ch, last, epoch in self.conn.execute(
                select(_CP.c.channel, _CP.c.last_seq, _CP.c.epoch).where(_CP.c.channel.in_(chans)).with_for_update()
            ).all():
                self.progress[ch] = _Progress(int(last), int(epoch), True, int(last))
            for ch in chans:
                self.progress.setdefault(ch, _Progress(0, 0, False, 0))
            for row in self.conn.execute(
                select(_M.c.merchant_id, _M.c.currency, _M.c.reserve_minor, _M.c.bank_account, _M.c.settled_minor,
                       _M.c.held_minor).where(_M.c.merchant_id.in_(chans)).with_for_update()
            ).all():
                self.books[row[0]] = MerchantBook(row[0], currency=row[1], reserve_minor=row[2], bank_account=row[3],
                                                  settled_minor=row[4], held_minor=row[5])

    # -- 3: per record --------------------------------------------------------------------
    def apply(self, rec: Record, msg: LedgerMessage | None) -> Outcome:
        if msg is None:
            return Outcome(rec.partition, rec.offset, "malformed", None, None)
        if msg.session_epoch < self.fence:
            return self._out(rec, msg, "stale_epoch")
        if msg.kind != "DATA":
            return self._out(rec, msg, "progress" if self._advance(msg) else self._classify(msg))
        event = msg.event or {}
        event_id = event.get("event_id")
        if not isinstance(event_id, str) or not isinstance(event.get("type"), str):
            return self._out(rec, msg, "malformed")
        if event_id not in self.inserted or event_id in self.seen:
            return self._out(rec, msg, "duplicate")
        self.seen.add(event_id)
        if not self._advance(msg):
            self.drop_pe.append(event_id)             # D18 / D22: leave no trace for a later in-order redelivery
            return self._out(rec, msg, self._classify(msg))

        book = self._book(msg.channel)
        try:
            result = rules.apply(book, event, self.lookup)
            event_time = _parse_time(event["event_time"])
        except (KeyError, TypeError, ValueError):
            self.outcomes[event_id] = "malformed"      # poison: seq consumed, nothing else changes
            return self._out(rec, msg, "malformed")
        for eff in result.effects:
            self._execute(eff, msg)
        after = result.book_after
        self.books[msg.channel] = after
        self.dirty_merchants.add(msg.channel)
        self.history.append({"merchant_id": msg.channel, "seq": msg.seq, "event_time": event_time,
                             "settled_minor": after.settled_minor, "held_minor": after.held_minor,
                             "reserve_minor": after.reserve_minor})
        self.outcomes[event_id] = result.outcome
        return self._out(rec, msg, result.outcome)

    def _advance(self, msg: LedgerMessage) -> bool:
        p = self.progress[msg.channel]
        if msg.seq != p.last_seq + 1 or msg.session_epoch < p.epoch:
            return False
        p.last_seq, p.epoch, p.changed = msg.end_seq, msg.session_epoch, True
        return True

    def _classify(self, msg: LedgerMessage) -> str:
        return "duplicate" if msg.end_seq <= self.progress[msg.channel].last_seq else "out_of_order"

    def _book(self, merchant_id: str) -> MerchantBook:
        b = self.books.get(merchant_id)
        if b is None:
            b = MerchantBook(merchant_id)            # reference defaults: currency "", reserve 0, account ""
            self.books[merchant_id] = b
            self.new_merchants.add(merchant_id)
        return b

    def _execute(self, eff, msg: LedgerMessage) -> None:
        if isinstance(eff, (OpenAuth, CloseAuth, UpsertCapture, AddRefunded)):
            self.row_merchant.setdefault(eff.event_id, msg.channel)
        if isinstance(eff, OpenAuth):
            self.auths[eff.event_id] = AuthRow(eff.event_id, eff.amount_minor, True)
            self.touched_auths.add(eff.event_id)
        elif isinstance(eff, CloseAuth):
            a = self.lookup.auth(eff.event_id)
            if a is not None:
                self.auths[eff.event_id] = AuthRow(a.event_id, a.amount_minor, False)
                self.touched_auths.add(eff.event_id)
        elif isinstance(eff, UpsertCapture):
            self.captures[eff.event_id] = CaptureRow(eff.event_id, eff.amount_minor, 0)
            self.touched_captures.add(eff.event_id)
        elif isinstance(eff, AddRefunded):
            c = self.lookup.capture(eff.event_id)
            if c is not None:
                self.captures[eff.event_id] = CaptureRow(c.event_id, c.amount_minor, c.refunded_minor + eff.amount_minor)
                self.touched_captures.add(eff.event_id)
        elif isinstance(eff, AddPlatformFee):
            self.fees[eff.currency] += eff.fee_minor
        elif isinstance(eff, RecordPayout):
            self.payouts.append({"payout_id": eff.payout_id, "merchant_id": msg.channel, "seq": msg.seq,
                                 "amount_minor": eff.amount_minor, "currency": eff.currency,
                                 "bank_account": eff.bank_account, "status": eff.status, "reason": eff.reason})
        elif isinstance(eff, (SetConfig, AdjustBalance)):
            pass  # reflected in book_after
        else:  # pragma: no cover - a new effect type must be wired here
            raise TypeError(f"unknown effect {eff!r}")

    @staticmethod
    def _out(rec: Record, msg: LedgerMessage, status: str) -> Outcome:
        return Outcome(rec.partition, rec.offset, status, msg.channel, msg.seq)

    # -- 4: write back --------------------------------------------------------------------
    def finish(self) -> None:
        c = self.conn
        for mid in self.new_merchants | self.dirty_merchants:
            b = self.books[mid]
            # merchants: locked rows for existing ones, plain insert for new ones in this batch
            stmt = pg_insert(_M).values(merchant_id=mid, currency=b.currency, reserve_minor=b.reserve_minor,
                                        bank_account=b.bank_account, settled_minor=b.settled_minor,
                                        held_minor=b.held_minor)
            c.execute(stmt.on_conflict_do_update(index_elements=[_M.c.merchant_id], set_={
                "currency": stmt.excluded.currency, "reserve_minor": stmt.excluded.reserve_minor,
                "bank_account": stmt.excluded.bank_account, "settled_minor": stmt.excluded.settled_minor,
                "held_minor": stmt.excluded.held_minor}))
        for ch, p in self.progress.items():
            if not p.changed:
                continue
            if p.existed:
                n = c.execute(_CP.update().where(_CP.c.channel == ch, _CP.c.last_seq == p.orig_last_seq)
                              .values(last_seq=p.last_seq, epoch=p.epoch, updated_at=text("now()"))).rowcount
            else:
                # rowcount is -1 for INSERT … ON CONFLICT DO NOTHING under psycopg3; count RETURNING rows
                n = len(c.execute(pg_insert(_CP).values(channel=ch, last_seq=p.last_seq, epoch=p.epoch)
                                  .on_conflict_do_nothing(index_elements=[_CP.c.channel])
                                  .returning(_CP.c.channel)).all())
            if n != 1:
                raise BatchConflict(f"channel_progress for {ch} moved under us")
        if self.touched_auths:
            stmt = pg_insert(_A)
            c.execute(stmt.on_conflict_do_update(index_elements=[_A.c.event_id],
                                                 set_={"is_open": stmt.excluded.is_open}),
                      [{"event_id": a.event_id, "merchant_id": self.row_merchant[a.event_id], "amount_minor": a.amount_minor,
                        "is_open": a.is_open} for a in (self.auths[i] for i in self.touched_auths) if a is not None])
        if self.touched_captures:
            stmt = pg_insert(_C)
            c.execute(stmt.on_conflict_do_update(index_elements=[_C.c.event_id],
                                                 set_={"refunded_minor": stmt.excluded.refunded_minor}),
                      [{"event_id": x.event_id, "merchant_id": self.row_merchant[x.event_id], "amount_minor": x.amount_minor,
                        "refunded_minor": x.refunded_minor} for x in (self.captures[i] for i in self.touched_captures)
                       if x is not None])
        if self.payouts:
            c.execute(pg_insert(_PAY).on_conflict_do_nothing(index_elements=[_PAY.c.payout_id]), self.payouts)
        if self.history:
            c.execute(pg_insert(_LH).on_conflict_do_nothing(index_elements=[_LH.c.merchant_id, _LH.c.seq]), self.history)
        if self.outcomes:
            c.execute(_PE.update().where(_PE.c.event_id == bindparam("eid")).values(outcome=bindparam("oc")),
                      [{"eid": k, "oc": v[:48]} for k, v in self.outcomes.items()])
        if self.drop_pe:
            c.execute(_PE.delete().where(_PE.c.event_id.in_(self.drop_pe)))
        for currency, fee in self.fees.items():
            if fee:
                stmt = pg_insert(_PF).values(currency=currency, shard=self.partition, total_minor=fee)
                c.execute(stmt.on_conflict_do_update(index_elements=[_PF.c.currency, _PF.c.shard],
                                                     set_={"total_minor": _PF.c.total_minor + stmt.excluded.total_minor}))
