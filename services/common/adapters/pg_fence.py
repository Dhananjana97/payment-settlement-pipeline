"""PgSessionFence: the single `session_lease` row records the newest gateway session_epoch (D2)."""
from __future__ import annotations

from sqlalchemy import Engine, text

_CLAIM = text("""
    UPDATE session_lease
       SET epoch = :epoch, holder = :holder, acquired_at = now(), heartbeat_at = now()
     WHERE singleton = 1 AND epoch < :epoch
""")
_CURRENT = text("SELECT epoch FROM session_lease WHERE singleton = 1")


class PgSessionFence:
    def __init__(self, engine: Engine):
        self._eng = engine

    def claim(self, epoch: int, holder: str) -> bool:
        with self._eng.begin() as conn:
            return conn.execute(_CLAIM, {"epoch": int(epoch), "holder": holder}).rowcount == 1

    def current(self) -> int:
        with self._eng.connect() as conn:
            return int(conn.execute(_CURRENT).scalar() or 0)
