"""PgExecutorRegistry (ExecutorRegistry port): live executors, for the limiter's local-share fallback."""
from __future__ import annotations

from sqlalchemy import Engine, text

_BEAT = text("""
INSERT INTO executor_heartbeat (instance, seen_at) VALUES (:i, now())
ON CONFLICT (instance) DO UPDATE SET seen_at = now()
""")
_LIVE = text("SELECT count(*) FROM executor_heartbeat WHERE seen_at > now() - make_interval(secs => :w)")
_PRUNE = text("DELETE FROM executor_heartbeat WHERE seen_at < now() - interval '1 hour'")


class PgExecutorRegistry:
    def __init__(self, engine: Engine):
        self._eng = engine

    def heartbeat(self, instance: str) -> None:
        with self._eng.begin() as conn:
            conn.execute(_BEAT, {"i": instance})
            conn.execute(_PRUNE)

    def live_count(self, within_s: float) -> int:
        with self._eng.connect() as conn:
            return int(conn.execute(_LIVE, {"w": float(within_s)}).scalar() or 0)
