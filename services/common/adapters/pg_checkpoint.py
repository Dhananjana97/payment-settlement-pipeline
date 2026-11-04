"""PgCheckpoint over `channel_progress` (D3). `load()` for the reader; `advance()` for the consumer.

Contiguity is enforced in SQL: a row only moves from `seq - 1` to `end_seq`. A channel with no row
yet is at 0, so only `seq == 1` may create it.
"""
from __future__ import annotations

from sqlalchemy import Connection, Engine, text

_LOAD = text("SELECT channel, last_seq FROM channel_progress")
_FIRST = text("""
    INSERT INTO channel_progress (channel, last_seq, epoch, updated_at)
    VALUES (:channel, :end_seq, :epoch, now())
    ON CONFLICT (channel) DO UPDATE
       SET last_seq = EXCLUDED.last_seq, epoch = EXCLUDED.epoch, updated_at = now()
     WHERE channel_progress.last_seq = 0 AND channel_progress.epoch <= EXCLUDED.epoch
""")
_NEXT = text("""
    UPDATE channel_progress
       SET last_seq = :end_seq, epoch = :epoch, updated_at = now()
     WHERE channel = :channel AND last_seq = :seq - 1 AND epoch <= :epoch
""")


class PgCheckpoint:
    def __init__(self, engine: Engine):
        self._eng = engine

    def load(self) -> dict[str, int]:
        with self._eng.connect() as conn:
            return {ch: int(last) for ch, last in conn.execute(_LOAD).all()}

    def advance(self, channel: str, seq: int, end_seq: int, epoch: int) -> bool:
        with self._eng.begin() as conn:
            return advance_in(conn, channel, seq, end_seq, epoch)


def advance_in(conn: Connection, channel: str, seq: int, end_seq: int, epoch: int) -> bool:
    """Same rule inside a caller-owned transaction (the consumer's apply batch)."""
    params = {"channel": channel, "seq": seq, "end_seq": end_seq, "epoch": epoch}
    stmt = _FIRST if seq == 1 else _NEXT
    return conn.execute(stmt, params).rowcount == 1
