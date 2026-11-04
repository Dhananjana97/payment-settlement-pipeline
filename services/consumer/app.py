"""ConsumerApp: poll → one apply transaction per partition batch → commit offsets (STEP3-PLAN §3.3).

Retries a batch on database connection errors with backoff and never exits on them (D20).
"""
from __future__ import annotations

import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field

from sqlalchemy.exc import DBAPIError, OperationalError

from services.consumer.adapters.pg_ledger_store import BatchConflict

from services.common import metrics
from services.common.clock import Clock, SystemClock
from services.common.log import BoundLogger
from services.consumer.ports import EventStream, LedgerStore, Outcome, Record


@dataclass
class ConsumerConfig:
    batch_max: int = 200
    batch_wait_s: float = 0.05
    fence_refresh_s: float = 2.0
    lag_refresh_s: float = 5.0
    retry_min_s: float = 0.2
    retry_max_s: float = 5.0
    group_id: str = "ledger"


@dataclass
class ConsumerState:
    last_batch_at: float | None = None
    last_error: str | None = None
    batches: int = 0
    records: int = 0
    retries: int = 0
    fence_epoch: int = 0
    lag: dict = field(default_factory=dict)


class ConsumerApp:
    def __init__(self, *, cfg: ConsumerConfig, stream: EventStream, store: LedgerStore, log: BoundLogger,
                 clock: Clock | None = None):
        self.cfg = cfg
        self._stream, self._store, self._log = stream, store, log
        self._clock = clock or SystemClock()
        self.state = ConsumerState()
        self._stop = threading.Event()
        self._fence_at = 0.0
        self._lag_at = 0.0

    def request_stop(self) -> None:
        self._stop.set()

    def readiness(self) -> tuple[bool, dict]:
        try:
            parts = self._stream.assignment()
        except Exception as e:
            return False, {"error": repr(e)}
        detail = {"partitions": parts, "last_batch_at": self.state.last_batch_at, "batches": self.state.batches,
                  "records": self.state.records, "retries": self.state.retries, "last_error": self.state.last_error,
                  "fence_epoch": self.state.fence_epoch, "lag": self.state.lag}
        return True, detail

    # -- main loop ------------------------------------------------------------------------
    def run(self) -> None:
        try:
            while not self._stop.is_set():
                self._export_lag()
                records = self._stream.poll(self.cfg.batch_max, self.cfg.batch_wait_s)
                if not records:
                    continue
                self._refresh_fence()
                groups: dict[int, list[Record]] = defaultdict(list)
                for r in records:
                    groups[r.partition].append(r)
                for partition in sorted(groups):
                    batch = groups[partition]
                    outcomes = self._apply_with_retry(partition, batch)
                    self._stream.commit(partition, batch[-1].offset + 1)
                    self._account(batch, outcomes)
        finally:
            try:
                self._stream.close()
            except Exception as e:  # pragma: no cover
                self._log.warning("stream close failed", error=repr(e))

    def _apply_with_retry(self, partition: int, batch: list[Record]) -> list[Outcome]:
        delay = self.cfg.retry_min_s
        while True:
            t0 = time.perf_counter()
            try:
                outcomes = self._store.apply_batch(partition, batch, self.state.fence_epoch)
                metrics.consumer_apply_seconds.observe(time.perf_counter() - t0)
                metrics.consumer_batches_total.labels(result="ok").inc()
                return outcomes
            except (OperationalError, DBAPIError, BatchConflict) as e:
                self.state.retries += 1
                self.state.last_error = repr(e)
                metrics.consumer_batches_total.labels(result="retry").inc()
                self._log.warning("apply batch failed; retrying", partition=partition, size=len(batch),
                                  delay_s=delay, error=str(e).splitlines()[0][:200])
                if self._stop.wait(delay):
                    raise          # shutting down: don't loop forever
                delay = min(delay * 2, self.cfg.retry_max_s)

    def _export_lag(self) -> None:
        """consumer_partition_lag{partition} every lag_refresh_s (A6: broker lag per partition).
        Optional on the stream: fakes without `lag()` simply don't export it."""
        now = self._clock.now()
        if now - self._lag_at < self.cfg.lag_refresh_s:
            return
        self._lag_at = now
        lag_fn = getattr(self._stream, "lag", None)
        if lag_fn is None:
            return
        try:
            lags = lag_fn()
        except Exception as e:  # observability must never stop the apply loop
            self._log.warning("lag read failed", error=repr(e)[:200])
            return
        for partition, value in lags.items():
            metrics.consumer_partition_lag.labels(partition=str(partition)).set(value)
        # drop partitions this process no longer owns (after a rebalance), so a revoked
        # partition doesn't keep reporting its last value next to the new owner's
        for partition in set(self.state.lag) - set(lags):
            try:
                metrics.consumer_partition_lag.remove(str(partition))
            except KeyError:
                pass
        self.state.lag = lags

    def _refresh_fence(self) -> None:
        now = self._clock.now()
        if now - self._fence_at < self.cfg.fence_refresh_s:
            return
        try:
            self.state.fence_epoch = self._store.fence_epoch()
            self._fence_at = now
        except (OperationalError, DBAPIError) as e:
            self._log.warning("fence read failed; keeping cached epoch", error=str(e).splitlines()[0][:200])

    def _account(self, batch: list[Record], outcomes: list[Outcome]) -> None:
        self.state.batches += 1
        self.state.records += len(batch)
        self.state.last_batch_at = self._clock.now()
        metrics.consumer_batch_size.observe(len(batch))
        for o in outcomes:
            metrics.consumer_events_total.labels(outcome=o.outcome_class).inc()
            if o.status == "stale_epoch":
                metrics.consumer_stale_epoch_total.inc()
            elif o.status == "out_of_order":
                metrics.consumer_out_of_order_total.inc()
                self._log.error("out-of-order message skipped", channel=o.channel, seq=o.seq,
                                partition=o.partition, offset=o.offset)
            elif o.status == "malformed":
                self._log.error("malformed message skipped", channel=o.channel, seq=o.seq,
                                partition=o.partition, offset=o.offset)
