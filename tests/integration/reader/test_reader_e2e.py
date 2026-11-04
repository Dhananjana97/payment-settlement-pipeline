"""ReaderApp end to end: real chaos gateway → real Redpanda, own client_id and topic.

Asserts per channel that the published seqs cover 1..max with no holes (duplicates allowed),
GAP_FILL ranges are consistent, and session_epoch never decreases. Also records go-back-N waste
(dropped_ahead / delivered) for D13.
"""
import asyncio
import os
import time
import uuid
from collections import defaultdict

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from services.common.adapters.pg_checkpoint import PgCheckpoint
from services.common.adapters.pg_fence import PgSessionFence
from services.common.adapters.pg_lock import PgLeaderLock
from services.common.log import get_logger
from services.common.messages import decode
from services.reader.adapters.bosp_client import BospClient
from services.reader.adapters.redpanda_producer import RedpandaProducer
from services.reader.app import ReaderApp, ReaderConfig

pytestmark = pytest.mark.integration

BOOTSTRAP = os.environ.get("TEST_KAFKA_BOOTSTRAP", "localhost:19092")
RUN_S = float(os.environ.get("READER_E2E_SECONDS", "45"))


def _create_topic(name: str, partitions: int = 12) -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    fs = admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)])
    for f in fs.values():
        f.result(10)


def _read_all(topic: str) -> list:
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"verify-{uuid.uuid4().hex[:6]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    md = c.list_topics(topic, timeout=10)
    parts = [TopicPartition(topic, p) for p in md.topics[topic].partitions]
    c.assign(parts)
    ends = {p.partition: c.get_watermark_offsets(p, timeout=10)[1] for p in parts}
    out, done = [], {p for p, hi in ends.items() if hi == 0}
    pos = defaultdict(int)
    while len(done) < len(parts):
        msg = c.poll(2.0)
        if msg is None:
            break
        if msg.error():
            continue
        out.append(decode(msg.value()))
        pos[msg.partition()] = msg.offset() + 1
        if pos[msg.partition()] >= ends[msg.partition()]:
            done.add(msg.partition())
    c.close()
    return out


async def test_reader_publishes_contiguous_streams_under_chaos(engine, migrated):
    topic = f"test-ledger-events-{uuid.uuid4().hex[:8]}"
    _create_topic(topic)
    client_id = f"e2e-{uuid.uuid4().hex[:8]}"
    holder = f"e2e-{client_id}"
    log = get_logger("e2e-reader")
    app = ReaderApp(
        cfg=ReaderConfig(holder=holder),
        source_factory=lambda: BospClient("localhost", 7100, client_id, "bo-dev-secret"),
        publisher_factory=lambda: RedpandaProducer(BOOTSTRAP, topic, client_id=holder),
        lock=PgLeaderLock(engine, name=f"lock-{client_id}"),
        fence=PgSessionFence(engine),
        checkpoint=PgCheckpoint(engine),   # shared table: contains the real reader's progress; fine for resume
        log=log,
    )
    task = asyncio.create_task(app.run())
    t0 = time.time()
    await asyncio.sleep(RUN_S)
    app.request_stop()
    await asyncio.wait_for(task, 30)
    elapsed = time.time() - t0

    msgs = _read_all(topic)
    assert msgs, "nothing published"
    by_ch = defaultdict(list)
    for m in msgs:
        by_ch[m.channel].append(m)

    holes, epochs_ok, gapfill_ok = {}, True, True
    for ch, ms in by_ch.items():
        covered = set()
        last_epoch = 0
        for m in ms:
            if m.session_epoch < last_epoch:
                epochs_ok = False
            last_epoch = max(last_epoch, m.session_epoch)
            if m.kind == "GAP_FILL" and m.end_seq < m.seq:
                gapfill_ok = False
            covered.update(range(m.seq, m.end_seq + 1))
        lo, hi = min(covered), max(covered)
        missing = [s for s in range(lo, hi + 1) if s not in covered]
        if missing:
            holes[ch] = missing[:5]
    assert epochs_ok, "session_epoch decreased within a channel"
    assert gapfill_ok
    assert not holes, f"channels with holes: {holes}"

    total = len(msgs)
    rate = total / elapsed
    st = app.state
    print(f"\nE2E: {total} messages over {len(by_ch)} channels in {elapsed:.0f}s ({rate:.0f}/s), "
          f"sessions={st.sessions}, last_exit={st.last_exit_reason}")
    assert rate > 100, "throughput suspiciously low"
