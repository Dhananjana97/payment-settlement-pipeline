import threading

from services.common.log import get_logger
from services.consumer.app import ConsumerApp, ConsumerConfig
from services.consumer.ports import Record
from tests.unit.consumer.fakes import FakeStore, InMemoryStream


def rec(partition, offset, key="M-0001"):
    return Record(partition, offset, key, b"{}")


def run(app: ConsumerApp, stream: InMemoryStream, timeout=5.0):
    t = threading.Thread(target=app.run, daemon=True)
    t.start()
    deadline = threading.Event()
    import time
    end = time.time() + timeout
    while not stream.drained and time.time() < end:
        time.sleep(0.005)
    time.sleep(0.05)   # let the last batch finish
    app.request_stop()
    t.join(timeout)
    assert not t.is_alive(), "app did not stop"


def make(stream, store, **cfg):
    base = dict(batch_wait_s=0.001, retry_min_s=0.005, retry_max_s=0.02)
    base.update(cfg)
    return ConsumerApp(cfg=ConsumerConfig(**base), stream=stream, store=store, log=get_logger("test-consumer"))


def test_batches_grouped_by_partition_and_committed_after_apply():
    records = [rec(1, 10), rec(0, 5), rec(1, 11), rec(0, 6), rec(2, 0)]
    stream, store = InMemoryStream(records), FakeStore(fence=42)
    app = make(stream, store)
    run(app, stream)
    assert store.applied_offsets() == {0: [5, 6], 1: [10, 11], 2: [0]}
    assert [p for p, _, _ in store.applied] == [0, 1, 2]                # ascending partition
    assert stream.commits == [(0, 7), (1, 12), (2, 1)]                    # last.offset + 1, once per batch
    assert all(f == 42 for _, _, f in store.applied)                      # fence passed through
    assert app.state.batches == 3 and app.state.records == 5
    assert stream.closed


def test_db_error_retries_batch_once_applied_once_committed_once():
    stream, store = InMemoryStream([rec(0, 1), rec(0, 2)]), FakeStore(fail_times=2)
    app = make(stream, store)
    run(app, stream)
    assert store.calls == 3                       # 2 failures + 1 success
    assert store.applied_offsets() == {0: [1, 2]}
    assert stream.commits == [(0, 3)]
    assert app.state.retries == 2 and "boom" in app.state.last_error


def test_poll_chunks_become_separate_batches_in_order():
    stream = InMemoryStream([rec(0, i) for i in range(10)], chunk=4)
    store = FakeStore()
    app = make(stream, store, batch_max=4)
    run(app, stream)
    assert [len(b) for _, b, _ in store.applied] == [4, 4, 2]
    assert stream.commits == [(0, 4), (0, 8), (0, 10)]


def test_stop_finishes_current_batch_then_closes():
    stream = InMemoryStream([rec(0, i) for i in range(3)])
    store = FakeStore()
    app = make(stream, store)
    run(app, stream)
    assert stream.commits == [(0, 3)] and stream.closed


def test_outcome_accounting_and_readiness():
    stream = InMemoryStream([rec(0, 1, "A"), rec(0, 2, "B"), rec(0, 3, "C")], parts=[0, 3])
    store = FakeStore(status_for={"A": "rejected:auth_not_open", "B": "out_of_order", "C": "stale_epoch"})
    app = make(stream, store)
    run(app, stream)
    ok, detail = app.readiness()
    assert ok and detail["partitions"] == [0, 3] and detail["records"] == 3 and detail["fence_epoch"] == 7


def test_stop_during_retry_backoff_exits():
    stream, store = InMemoryStream([rec(0, 1)]), FakeStore(fail_times=10_000)
    app = make(stream, store, retry_min_s=0.5, retry_max_s=0.5)
    t = threading.Thread(target=app.run, daemon=True)
    t.start()
    import time
    time.sleep(0.05)
    app.request_stop()
    t.join(2)
    assert not t.is_alive()
    assert stream.commits == []           # never committed a batch that did not apply
