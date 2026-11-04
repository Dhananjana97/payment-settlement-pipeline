"""PgPayoutRepository on real Postgres (ledger_test): claim, per-account cap, leases, CAS."""
import threading
import time

import pytest
from sqlalchemy import text

from services.executor.adapters.pg_payout_repo import PgPayoutRepository, TransitionFieldError

pytestmark = pytest.mark.integration


def seed(engine, rows):
    """rows: (payout_id, account, status, created_offset_s)."""
    with engine.begin() as c:
        for pid, acct, status, off in rows:
            c.execute(text("""
                INSERT INTO payouts (payout_id, merchant_id, seq, amount_minor, currency, bank_account, status,
                                     created_at, updated_at)
                VALUES (:p, :m, 1, 1000, 'USD', :a, :s, now() + make_interval(secs => :o), now())
            """), {"p": pid, "m": acct.replace("ACCT", "M"), "a": acct, "s": status, "o": off})


@pytest.fixture
def repo(engine, clean_tables):
    return PgPayoutRepository(engine)


def test_claim_returns_snapshots_oldest_first(repo, engine):
    seed(engine, [("P:2", "ACCT-A", "accepted", -5), ("P:1", "ACCT-B", "accepted", -10), ("P:3", "ACCT-C", "rejected", -20)])
    got = repo.claim("e1", 30, limit=10, per_account=2)
    assert [p.payout_id for p in got] == ["P:1", "P:2"]          # rejected is not claimable
    p = got[0]
    assert p.status == "claimed" and p.lease_owner == "e1" and p.attempt == 0
    assert p.amount_minor == 1000 and p.currency == "USD" and p.bank_account == "ACCT-B"
    assert p.key_uncertain is False and p.pending_send_at is None and p.search_attempts == 0
    assert repo.claim("e2", 30, limit=10, per_account=2) == []    # all leased


def test_concurrent_claims_are_disjoint(engine, clean_tables):
    seed(engine, [(f"P:{i}", f"ACCT-{i % 40}", "accepted", -i) for i in range(200)])
    results: dict[str, list[str]] = {}
    barrier = threading.Barrier(4)

    def worker(name):
        r = PgPayoutRepository(engine)
        barrier.wait()
        mine = []
        for _ in range(10):
            mine += [p.payout_id for p in r.claim(name, 60, limit=8, per_account=2)]
        results[name] = mine

    ts = [threading.Thread(target=worker, args=(f"e{i}",)) for i in range(4)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    allp = [p for v in results.values() for p in v]
    assert len(allp) == len(set(allp)), "a payout was claimed twice"
    assert len(allp) == 80                                         # 40 accounts × 2 per account
    with engine.connect() as c:
        owners = dict(c.execute(text("SELECT payout_id, lease_owner FROM payouts WHERE lease_owner IS NOT NULL")).all())
    for name, mine in results.items():
        assert all(owners[p] == name for p in mine)


def test_per_account_cap_counts_already_leased_rows(repo, engine):
    seed(engine, [(f"P:{i}", "ACCT-HOT", "accepted", -10 + i) for i in range(6)] + [("Q:1", "ACCT-X", "accepted", 0)])
    first = repo.claim("e1", 60, limit=1, per_account=2)
    assert [p.payout_id for p in first] == ["P:0"]
    second = repo.claim("e2", 60, limit=10, per_account=2)
    assert sorted(p.payout_id for p in second) == ["P:1", "Q:1"]   # only one more for ACCT-HOT
    assert repo.claim("e3", 60, limit=10, per_account=2) == []


def test_expired_lease_reclaimed_status_preserved(repo, engine):
    seed(engine, [("P:1", "ACCT-A", "accepted", 0)])
    (p,) = repo.claim("e1", 30, limit=1, per_account=2)
    assert repo.transition("P:1", "e1", "claimed", 0, {
        "status": "in_flight", "attempt": 1, "idempotency_key": "P:1#a1", "key_first_sent_at": time.time(),
        "last_sent_at": time.time(), "pending_send_at": time.time()})
    with engine.begin() as c:
        c.execute(text("UPDATE payouts SET lease_until = now() - interval '1 s' WHERE payout_id = 'P:1'"))
    (q,) = repo.claim("e2", 30, limit=1, per_account=2)
    assert q.payout_id == "P:1" and q.status == "in_flight" and q.lease_owner == "e2"
    assert q.attempt == 1 and q.idempotency_key == "P:1#a1" and q.pending_send_at is not None
    assert repo.renew("P:1", "e1", 30) is False                     # old owner lost it
    assert repo.renew("P:1", "e2", 30) is True


def test_transition_cas(repo, engine):
    seed(engine, [("P:1", "ACCT-A", "accepted", 0)])
    repo.claim("e1", 30, limit=1, per_account=2)
    assert repo.transition("P:1", "e2", "claimed", 0, {"status": "in_flight", "attempt": 1}) is False   # stale owner
    assert repo.transition("P:1", "e1", "in_flight", 0, {"status": "paid"}) is False                    # wrong status
    assert repo.transition("P:1", "e1", "claimed", 3, {"status": "in_flight"}) is False                 # wrong attempt
    assert repo.transition("P:1", "e1", "claimed", 0, {"status": "in_flight", "attempt": 1,
                                                        "idempotency_key": "P:1#a1", "pending_send_at": time.time()})
    assert repo.transition("P:1", "e1", "in_flight", 1, {"status": "paid", "bank_payout_id": "BP-1",
                                                          "pending_send_at": None, "key_uncertain": False})
    with engine.connect() as c:
        row = c.execute(text("SELECT status, bank_payout_id, pending_send_at FROM payouts WHERE payout_id='P:1'")).one()
    assert row == ("paid", "BP-1", None)
    with pytest.raises(TransitionFieldError):
        repo.transition("P:1", "e1", "paid", 1, {"amount_minor": 1})


def test_release_schedules_next_attempt(repo, engine):
    seed(engine, [("P:1", "ACCT-A", "accepted", 0)])
    repo.claim("e1", 30, limit=1, per_account=2)
    with engine.connect() as c:
        db_now = c.execute(text("SELECT extract(epoch FROM now())")).scalar()
    repo.release("P:1", "e1", float(db_now) + 1.5)
    assert repo.claim("e2", 30, limit=1, per_account=2) == []       # not due yet
    time.sleep(1.7)
    (p,) = repo.claim("e2", 30, limit=1, per_account=2)
    assert p.payout_id == "P:1" and p.status == "claimed"


def test_stuck_is_never_claimed_and_counts(repo, engine):
    seed(engine, [("P:1", "ACCT-A", "stuck", 0), ("P:2", "ACCT-B", "paid", 0), ("P:3", "ACCT-C", "accepted", 0)])
    assert [p.payout_id for p in repo.claim("e1", 30, 10, 2)] == ["P:3"]
    assert repo.counts_by_status() == {"stuck": 1, "paid": 1, "claimed": 1}


def test_record_attempt(repo, engine):
    seed(engine, [("P:1", "ACCT-A", "accepted", 0)])
    repo.record_attempt("P:1", 1, "P:1#a1", 201, None)
    repo.record_attempt("P:1", 1, "P:1#a1", None, "no_response:ReadTimeout")
    with engine.connect() as c:
        rows = c.execute(text("SELECT http_status, error FROM payout_attempts WHERE payout_id='P:1' ORDER BY id")).all()
    assert rows == [(201, None), (None, "no_response:ReadTimeout")]
