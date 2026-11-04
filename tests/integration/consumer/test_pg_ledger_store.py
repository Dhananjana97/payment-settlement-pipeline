"""PgLedgerStore on real Postgres: exactly-once under concurrent redelivery, crash replay,
out-of-order skip + later apply, stale epoch, history rows, sharded fees (STEP3-PLAN §4)."""
import sys
import threading
import time
from pathlib import Path

import pytest
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "harness"))
import ledger_events as le  # noqa: E402  (allowed in tests, ASSIGNMENT.md:127)

from services.common.messages import LedgerMessage, encode  # noqa: E402
from services.consumer.adapters.pg_ledger_store import PgLedgerStore  # noqa: E402
from services.consumer.ports import Record  # noqa: E402

pytestmark = pytest.mark.integration

SEED, CH, PART, EPOCH = 42, "M-0007", 3, 100


def frames(n: int, seed: int = SEED, ch: str = CH):
    return list(le.channel_frames(seed, ch, n))


def to_record(f, offset, ch=CH, epoch=EPOCH, partition=PART) -> Record:
    kind = "DATA" if f["kind"] == "DATA" else "NOTICE"
    m = LedgerMessage(channel=ch, seq=f["seq"], end_seq=f["seq"], kind=kind,
                      event=f["event"] if kind == "DATA" else None, session_epoch=epoch, published_at=0.0)
    return Record(partition, offset, ch, encode(m))


def reference(n: int, ch: str = CH):
    book, platform, payouts = le.MerchantBook(ch), {}, {}
    outcomes = []
    for f in frames(n, ch=ch):
        if f["kind"] == "DATA":
            outcomes.append(le.apply_event(book, f["event"], platform, payouts))
    return book, platform, payouts, outcomes


def snapshot(engine, ch=CH):
    with engine.connect() as c:
        m = c.execute(text("SELECT currency, reserve_minor, bank_account, settled_minor, held_minor "
                           "FROM merchants WHERE merchant_id = :m"), {"m": ch}).first()
        last = c.execute(text("SELECT last_seq FROM channel_progress WHERE channel = :m"), {"m": ch}).scalar()
        n_pe = c.execute(text("SELECT count(*) FROM processed_events WHERE channel = :m"), {"m": ch}).scalar()
        n_lh = c.execute(text("SELECT count(*) FROM ledger_history WHERE merchant_id = :m"), {"m": ch}).scalar()
        fees = dict(c.execute(text("SELECT currency || ':' || shard, total_minor FROM platform_fees")).all())
        pays = {r[0]: (r[1], r[2], r[3], r[4]) for r in c.execute(text(
            "SELECT payout_id, status, amount_minor, bank_account, currency FROM payouts WHERE merchant_id = :m"),
            {"m": ch}).all()}
    return {"merchant": tuple(m) if m else None, "last_seq": last, "processed": n_pe, "history": n_lh,
            "fees": fees, "payouts": pays}


@pytest.fixture
def store(engine, migrated, clean_tables):
    with engine.begin() as c:
        c.execute(text("UPDATE session_lease SET epoch = 0 WHERE singleton = 1"))
    return PgLedgerStore(engine)


def assert_matches_reference(engine, n):
    book, platform, payouts, _ = reference(n)
    s = snapshot(engine)
    assert s["merchant"] == (book.currency, book.reserve, book.bank_account, book.settled, book.held)
    assert s["last_seq"] == n
    data = sum(1 for f in frames(n) if f["kind"] == "DATA")
    assert s["processed"] == data and s["history"] == data
    assert {k.split(":")[0]: v for k, v in s["fees"].items()} == platform
    assert all(k.endswith(f":{PART}") for k in s["fees"])            # shard = partition
    assert s["payouts"] == {pid: (p["status"], p["amount_minor"], p["account"], p["currency"])
                            for pid, p in payouts.items()}


def test_single_apply_matches_reference_and_outcomes(store, engine):
    n = 300
    recs = [to_record(f, i) for i, f in enumerate(frames(n))]
    outcomes = store.apply_batch(PART, recs, fence_epoch=EPOCH)
    _, _, _, ref_outcomes = reference(n)
    got = [o.status for o in outcomes if o.status != "progress"]
    assert got == ref_outcomes
    assert sum(1 for o in outcomes if o.status == "progress") == sum(1 for f in frames(n) if f["kind"] == "NOTICE")
    assert_matches_reference(engine, n)


def test_same_batch_in_two_threads_applies_once(store, engine):
    n = 120
    recs = [to_record(f, i) for i, f in enumerate(frames(n))]
    results, errors = {}, []
    barrier = threading.Barrier(2)

    def worker(name):
        try:
            barrier.wait()
            results[name] = store.apply_batch(PART, recs, fence_epoch=EPOCH)
        except Exception as e:  # pragma: no cover
            errors.append(repr(e))

    ts = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(60)
    assert not errors
    statuses = [[o.status for o in results[i]] for i in (0, 1)]
    real = {"applied", "ignored"} | {s for s in sum(statuses, []) if s.startswith("rejected:")}
    applied_by = [sum(1 for s in st if s in real or s == "progress") for st in statuses]
    # one thread did all the work, the other saw only duplicates (the row lock serialises them)
    assert sorted(applied_by) == [0, n]
    loser = statuses[applied_by.index(0)]
    assert set(loser) == {"duplicate"}
    assert_matches_reference(engine, n)


def test_crash_mid_batch_leaves_nothing_then_replays_cleanly(store, engine):
    n = 200
    recs = [to_record(f, i) for i, f in enumerate(frames(n))]
    clean_store = PgLedgerStore(engine)

    class Boom(RuntimeError):
        pass

    # raise on the 4th statement of the batch (after dedupe insert + row locks, before any write-back):
    # mimics kill -9 mid-batch
    from sqlalchemy import event as sa_event
    counter = {"n": 0}

    def before_exec(conn, clauseelement, multiparams, params, execution_options):
        counter["n"] += 1
        if counter["n"] == 4:
            raise Boom("crash")

    sa_event.listen(engine, "before_execute", before_exec)
    try:
        with pytest.raises(Boom):
            clean_store.apply_batch(PART, recs, fence_epoch=EPOCH)
    finally:
        sa_event.remove(engine, "before_execute", before_exec)
    s = snapshot(engine)
    assert s["processed"] == 0 and s["history"] == 0 and s["merchant"] is None and s["last_seq"] is None

    clean_store.apply_batch(PART, recs, fence_epoch=EPOCH)          # redelivery
    assert_matches_reference(engine, n)
    clean_store.apply_batch(PART, recs, fence_epoch=EPOCH)          # and again: all duplicates
    assert_matches_reference(engine, n)


def test_out_of_order_is_skipped_without_trace_then_applies_in_order(store, engine):
    fs = frames(6)
    r = {f["seq"]: to_record(f, f["seq"]) for f in fs}
    out = store.apply_batch(PART, [r[1], r[2], r[4]], fence_epoch=EPOCH)
    assert out[2].status == "out_of_order"
    with engine.connect() as c:
        assert c.execute(text("SELECT count(*) FROM processed_events WHERE seq = 4")).scalar() == 0
        assert c.execute(text("SELECT last_seq FROM channel_progress WHERE channel = :m"), {"m": CH}).scalar() == 2
    out = store.apply_batch(PART, [r[3], r[4], r[5], r[6]], fence_epoch=EPOCH)
    assert all(o.status not in ("out_of_order", "duplicate") for o in out)
    assert_matches_reference(engine, 6)


def test_stale_epoch_skipped_and_nothing_written(store, engine):
    fs = frames(3)
    recs = [to_record(f, i, epoch=9) for i, f in enumerate(fs)]
    out = store.apply_batch(PART, recs, fence_epoch=10)
    assert [o.status for o in out] == ["stale_epoch"] * 3
    s = snapshot(engine)
    assert s["processed"] == 0 and s["last_seq"] is None and s["merchant"] is None
    # same frames with a current epoch apply
    store.apply_batch(PART, [to_record(f, i, epoch=10) for i, f in enumerate(fs)], fence_epoch=10)
    assert_matches_reference(engine, 3)


def test_history_row_for_rejected_and_ignored_events(store, engine):
    base = {"merchant_id": CH, "currency": "USD"}
    evs = [
        {**base, "event_id": f"{CH}:1", "type": "MERCHANT_CONFIG", "event_time": "2026-11-01T10:00:00.000Z",
         "reserve_minor": 0, "bank_account": "ACCT-1"},
        {**base, "event_id": f"{CH}:2", "type": "REFUND", "event_time": "2026-11-01T10:00:01.000Z",
         "ref": "nope", "amount_minor": 5},                                    # rejected:capture_not_found
        {**base, "event_id": f"{CH}:3", "type": "LOYALTY_ACCRUAL", "event_time": "2026-11-01T10:00:02.000Z",
         "points": 3},                                                          # ignored
        {**base, "event_id": f"{CH}:4", "type": "AUTH", "event_time": "2026-11-01T10:00:03.000Z",
         "amount_minor": 700},
    ]
    recs = [Record(PART, i, CH, encode(LedgerMessage(CH, i + 1, i + 1, "DATA", ev, EPOCH, 0.0)))
            for i, ev in enumerate(evs)]
    out = store.apply_batch(PART, recs, fence_epoch=EPOCH)
    assert [o.status for o in out] == ["applied", "rejected:capture_not_found", "ignored", "applied"]
    with engine.connect() as c:
        rows = c.execute(text("SELECT seq, settled_minor, held_minor FROM ledger_history WHERE merchant_id = :m ORDER BY seq"),
                         {"m": CH}).all()
        assert rows == [(1, 0, 0), (2, 0, 0), (3, 0, 0), (4, 0, 700)]
        outcomes = dict(c.execute(text("SELECT seq, outcome FROM processed_events WHERE channel = :m"), {"m": CH}).all())
        assert outcomes == {1: "applied", 2: "rejected:capture_not_found", 3: "ignored", 4: "applied"}


def test_malformed_and_poison_messages_do_not_block(store, engine):
    good = to_record(frames(1)[0], 0)
    garbage = Record(PART, 1, CH, b"not json")
    poison = Record(PART, 2, CH, encode(LedgerMessage(CH, 2, 2, "DATA", {"event_id": f"{CH}:2", "type": "AUTH",
                                                                           "merchant_id": CH, "currency": "USD",
                                                                           "event_time": "2026-11-01T10:00:01.000Z"},
                                                        EPOCH, 0.0)))        # AUTH without amount_minor
    out = store.apply_batch(PART, [good, garbage, poison], fence_epoch=EPOCH)
    # undecodable bytes -> malformed (no seq to consume); a decodable event missing fields is rejected by
    # the rules (rejected:malformed_event) and consumes its seq like any other rejection
    assert [o.status for o in out] == ["applied", "malformed", "rejected:malformed_event"]
    with engine.connect() as c:
        assert c.execute(text("SELECT last_seq FROM channel_progress WHERE channel = :m"), {"m": CH}).scalar() == 2
        assert c.execute(text("SELECT outcome FROM processed_events WHERE seq = 2")).scalar() == "rejected:malformed_event"
        assert c.execute(text("SELECT count(*) FROM ledger_history WHERE merchant_id = :m"), {"m": CH}).scalar() == 2


def test_two_merchants_same_partition_and_fence_read(store, engine):
    a = [to_record(f, i, ch="M-0007") for i, f in enumerate(frames(40, ch="M-0007"))]
    b = [to_record(f, 100 + i, ch="M-0008") for i, f in enumerate(frames(40, ch="M-0008"))]
    inter = [x for pair in zip(a, b) for x in pair]
    store.apply_batch(PART, inter, fence_epoch=EPOCH)
    for ch in ("M-0007", "M-0008"):
        book, *_ = reference(40, ch=ch)
        s = snapshot(engine, ch)
        assert s["merchant"] == (book.currency, book.reserve, book.bank_account, book.settled, book.held)
        assert s["last_seq"] == 40
    with engine.begin() as c:
        c.execute(text("UPDATE session_lease SET epoch = 123 WHERE singleton = 1"))
    assert store.fence_epoch() == 123


def test_throughput_smoke(store, engine):
    n = 2000
    recs = [to_record(f, i) for i, f in enumerate(frames(n))]
    t0 = time.perf_counter()
    for i in range(0, n, 200):
        store.apply_batch(PART, recs[i:i + 200], fence_epoch=EPOCH)
    dt = time.perf_counter() - t0
    print(f"\nstore throughput: {n / dt:.0f} events/s single-threaded, batches of 200")
    assert_matches_reference(engine, n)
    assert n / dt > 300
