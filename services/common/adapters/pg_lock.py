"""PgLeaderLock: a lease row in `leader_lock` (D12, D20).

Uses the database clock (`now()`) on both sides so reader clock skew cannot matter.
"""
from __future__ import annotations

from sqlalchemy import Engine, text
from sqlalchemy.exc import DBAPIError, OperationalError

from services.common.ports import RenewResult

_ACQUIRE = text("""
    INSERT INTO leader_lock (name, holder, expires_at)
    VALUES (:name, :holder, now() + make_interval(secs => :ttl))
    ON CONFLICT (name) DO UPDATE
       SET holder = EXCLUDED.holder, expires_at = EXCLUDED.expires_at
     WHERE leader_lock.expires_at < now() OR leader_lock.holder = EXCLUDED.holder
""")
_RENEW = text("""
    UPDATE leader_lock SET expires_at = now() + make_interval(secs => :ttl)
     WHERE name = :name AND holder = :holder AND expires_at > now()
""")
_RELEASE = text("UPDATE leader_lock SET expires_at = now() WHERE name = :name AND holder = :holder")
_HOLDER = text("SELECT holder FROM leader_lock WHERE name = :name AND expires_at > now()")


class PgLeaderLock:
    def __init__(self, engine: Engine, name: str = "reader"):
        self._eng = engine
        self._name = name

    def try_acquire(self, holder: str, ttl_s: float) -> bool:
        try:
            with self._eng.begin() as conn:
                res = conn.execute(_ACQUIRE, {"name": self._name, "holder": holder, "ttl": float(ttl_s)})
                return res.rowcount == 1
        except (OperationalError, DBAPIError):
            return False

    def renew(self, holder: str, ttl_s: float) -> RenewResult:
        try:
            with self._eng.begin() as conn:
                res = conn.execute(_RENEW, {"name": self._name, "holder": holder, "ttl": float(ttl_s)})
                return RenewResult.RENEWED if res.rowcount == 1 else RenewResult.LOST
        except (OperationalError, DBAPIError):
            return RenewResult.UNAVAILABLE

    def release(self, holder: str) -> None:
        try:
            with self._eng.begin() as conn:
                conn.execute(_RELEASE, {"name": self._name, "holder": holder})
        except (OperationalError, DBAPIError):
            pass  # lease will expire on its own

    def current_holder(self) -> str | None:
        with self._eng.connect() as conn:
            return conn.execute(_HOLDER, {"name": self._name}).scalar()
