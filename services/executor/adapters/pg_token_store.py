"""PgTokenStore (TokenStore port): the one shared OAuth token in the `bank_token` row (D6 rev., B §1).

Mint slot is claimed by CAS on the row: only one executor can win it per 5 s (the bank's own
issuance limit, bank.py:161), and only if the token it saw fail is still the stored one, so two
executors that both got a 401 produce exactly one mint. A minter that crashes between the CAS and
`set()` loses nothing: the claim ages out in 5 s.
"""
from __future__ import annotations

from sqlalchemy import Engine, text

_GET = text("SELECT token, extract(epoch FROM expires_at) FROM bank_token WHERE singleton = 1")
_CLAIM = text("""
UPDATE bank_token
   SET minted_by = :holder, issued_at = now()
 WHERE singleton = 1
   AND (issued_at IS NULL OR issued_at < now() - make_interval(secs => :min_interval))
   AND token IS NOT DISTINCT FROM :seen
""")
_SET = text("""
UPDATE bank_token
   SET token = :token, expires_at = now() + make_interval(secs => :ttl), minted_by = :holder
 WHERE singleton = 1
""")
_ENSURE = text("INSERT INTO bank_token (singleton) VALUES (1) ON CONFLICT DO NOTHING")
_NOW = text("SELECT extract(epoch FROM now())")

# Seconds subtracted from the bank's `expires_in` to absorb request latency (B §1.1).
EXPIRY_SAFETY_S = 2


class PgTokenStore:
    def __init__(self, engine: Engine):
        self._eng = engine
        with self._eng.begin() as conn:
            conn.execute(_ENSURE)

    def get(self) -> tuple[str, float] | None:
        with self._eng.connect() as conn:
            row = conn.execute(_GET).first()
        if row is None or row[0] is None or row[1] is None:
            return None
        return row[0], float(row[1])

    def try_claim_mint(self, holder: str, seen_token: str | None, min_interval_s: float) -> bool:
        with self._eng.begin() as conn:
            res = conn.execute(_CLAIM, {"holder": holder, "seen": seen_token, "min_interval": float(min_interval_s)})
            return res.rowcount == 1

    def set(self, token: str, expires_in_s: int, minted_by: str) -> None:
        ttl = max(1, int(expires_in_s) - EXPIRY_SAFETY_S)
        with self._eng.begin() as conn:
            conn.execute(_SET, {"token": token, "ttl": ttl, "holder": minted_by})

    def now(self) -> float:
        with self._eng.connect() as conn:
            return float(conn.execute(_NOW).scalar())
