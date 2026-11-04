"""ConsumerApp end to end: generated channel → RedpandaProducer → real topic → RedpandaConsumer →
PgLedgerStore, compared with ledger_events.apply_event."""
import os
import sys
import threading
import time
import uuid
from pathlib import Path

import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "harness"))
import ledger_events as le  # noqa: E402

from services.common.log import get_logger  # noqa: E402
from services.common.messages import LedgerMessage, encode  # noqa: E402
from services.consumer.adapters.pg_ledger_store import PgLedgerStore  # noqa: E402
from services.consumer.adapters.redpanda_consumer import RedpandaConsumer  # noqa: E402
from services.consumer.app import ConsumerApp, ConsumerConfig  # noqa: E402
from services.reader.adapters.redpanda_producer import RedpandaProducer  # noqa: E402

pytestmark = pytest.mark.integration

BOOTSTRAP = os.environ.get("TEST_KAFKA_BOOTSTRAP", "localhost:19092")
SEED, EPOCH = 42, 500
CHANNELS = ("M-0003", "M-0011", "M-0150")
N = 300


def test_consumer_app_applies_generated_channels(engine, migrated, clean_tables):
    topic = f"test-consumer-{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})      # keep a reference until the future resolves
    admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])[topic].result(10)
    with engine.begin() as c:
        c.execute(text("UPDATE session_lease SET epoch = :e WHERE singleton = 1"), {"e": EPOCH})

    # publish, with a few duplicates sprinkled in (at-least-once upstream)
    prod = RedpandaProducer(BOOTSTRAP, topic, client_id="e2e-pub")
    errors = []
    total = 0
    for ch in CHANNELS:
        for f in le.channel_frames(SEED, ch, N):
            kind = "DATA" if f["kind"] == "DATA" else "NOTICE"
            m = LedgerMessage(ch, f["seq"], f["seq"], kind, f["event"] if kind == "DATA" else None, EPOCH, time.time())
            times = 2 if f["seq"] % 37 == 0 else 1
            for _ in range(times):
                prod.publish(ch, encode(m), lambda err: errors.append(err) if err else None)
                total += 1
    assert prod.flush(30) == 0 and not errors

    app = ConsumerApp(
        cfg=ConsumerConfig(batch_max=100, batch_wait_s=0.2),
        stream=RedpandaConsumer(BOOTSTRAP, topic, group_id=f"g-{uuid.uuid4().hex[:6]}", client_id="e2e-consumer"),
        store=PgLedgerStore(engine), log=get_logger("e2e-consumer"))
    t = threading.Thread(target=app.run, daemon=True)
    t.start()
    deadline = time.time() + 90
    while app.state.records < total and time.time() < deadline:
        time.sleep(0.2)
    app.request_stop()
    t.join(30)
    assert app.state.records == total, f"consumed {app.state.records}/{total}"

    for ch in CHANNELS:
        book, platform, payouts = le.MerchantBook(ch), {}, {}
        for f in le.channel_frames(SEED, ch, N):
            if f["kind"] == "DATA":
                le.apply_event(book, f["event"], platform, payouts)
        with engine.connect() as c:
            m = c.execute(text("SELECT currency, reserve_minor, bank_account, settled_minor, held_minor "
                               "FROM merchants WHERE merchant_id = :m"), {"m": ch}).first()
            last = c.execute(text("SELECT last_seq FROM channel_progress WHERE channel = :m"), {"m": ch}).scalar()
            pays = {r[0]: r[1] for r in c.execute(text("SELECT payout_id, status FROM payouts WHERE merchant_id = :m"),
                                                    {"m": ch}).all()}
        assert m is not None, f"{ch}: no merchant row; last_seq={last}"
        assert tuple(m) == (book.currency, book.reserve, book.bank_account, book.settled, book.held), ch
        assert last == N, ch
        assert pays == {pid: p["status"] for pid, p in payouts.items()}, ch
    print(f"\nE2E consumer: {total} messages ({app.state.batches} batches) in "
          f"{app.state.last_batch_at - (deadline - 90):.1f}s")
